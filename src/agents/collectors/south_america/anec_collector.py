"""
ANEC Collector (Associação Nacional dos Exportadores de Cereais)

Collects weekly accumulated export data from ANEC - Brazil's grain exporters
association. ANEC publishes weekly PDF reports tracking shipment volumes for:
- Soja (Soybeans)
- Farelo de Soja (Soybean Meal)
- Milho (Corn)
- Trigo (Wheat)

Data source:
- https://anec.com.br
- URL pattern: https://anec.com.br/article/anec-exportacoes-acumuladas-{WW}{YYYY}
  where WW = ISO week number (01-52), YYYY = year

ANEC data is widely referenced by Reuters, Bloomberg, and ag market analysts
for tracking Brazil's grain export pace vs. prior years.

No API key required - data via web articles and PDF downloads.
"""

import logging
import re
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from io import BytesIO

from .base_collector import (
    BaseCollector,
    CollectorConfig,
    CollectorResult,
    DataFrequency,
    AuthType
)

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False

try:
    import pdfplumber
    PDFPLUMBER_AVAILABLE = True
except ImportError:
    PDFPLUMBER_AVAILABLE = False

logger = logging.getLogger(__name__)


# ANEC commodity mappings
ANEC_COMMODITIES = {
    'soja': {
        'en_name': 'soybeans',
        'code': 'SOYBEANS',
        'unit': 'tonnes',
    },
    'farelo_de_soja': {
        'en_name': 'soybean_meal',
        'code': 'SOYBEAN_MEAL',
        'unit': 'tonnes',
    },
    'milho': {
        'en_name': 'corn',
        'code': 'CORN',
        'unit': 'tonnes',
    },
    'trigo': {
        'en_name': 'wheat',
        'code': 'WHEAT',
        'unit': 'tonnes',
    },
}

# Keywords used to identify commodities in Portuguese PDF text
COMMODITY_KEYWORDS = {
    'soybeans': ['soja', 'soy'],
    'soybean_meal': ['farelo de soja', 'farelo', 'soybean meal', 'soymeal'],
    'corn': ['milho', 'corn', 'maize'],
    'wheat': ['trigo', 'wheat'],
}

# First year of available ANEC reports
ANEC_FIRST_YEAR = 2022


@dataclass
class ANECConfig(CollectorConfig):
    """ANEC specific configuration"""
    source_name: str = "ANEC"
    source_url: str = "https://anec.com.br"
    auth_type: AuthType = AuthType.NONE
    frequency: DataFrequency = DataFrequency.WEEKLY

    # ANEC article base URL
    article_base: str = "https://anec.com.br/article"

    # Commodities to track
    commodities: List[str] = field(default_factory=lambda: [
        'soybeans', 'soybean_meal', 'corn', 'wheat'
    ])

    # PDF download settings
    pdf_cache_dir: Path = field(default_factory=lambda: Path("./data/cache/anec_pdfs"))
    download_pdfs: bool = True

    # Rate limiting - be respectful
    rate_limit_per_minute: int = 6
    timeout: int = 60

    # Historical range
    start_year: int = ANEC_FIRST_YEAR


class ANECCollector(BaseCollector):
    """
    Collector for ANEC weekly accumulated export reports.

    ANEC (Associação Nacional dos Exportadores de Cereais) is Brazil's
    national association of cereal exporters. They publish weekly reports
    on accumulated grain export volumes.

    Key data:
    - Weekly accumulated exports for soybeans, soybean meal, corn, wheat
    - Year-to-date (YTD) totals
    - Monthly projections and comparisons vs. prior years
    - Port-level breakdown (Santos, Paranaguá, etc.)

    URL pattern: https://anec.com.br/article/anec-exportacoes-acumuladas-{WW}{YYYY}

    No API key required - web scraping of published articles and PDF reports.
    """

    def __init__(self, config: ANECConfig = None):
        config = config or ANECConfig()
        super().__init__(config)
        self.config: ANECConfig = config

        # Ensure PDF cache directory exists
        if self.config.download_pdfs:
            self.config.pdf_cache_dir.mkdir(parents=True, exist_ok=True)

        # Override default User-Agent with browser-like headers
        self.session.headers.update({
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/120.0.0.0 Safari/537.36'
            ),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        })

    def get_table_name(self) -> str:
        return "anec_weekly_exports"

    # =========================================================================
    # MAIN FETCH INTERFACE
    # =========================================================================

    def fetch_data(
        self,
        start_date: date = None,
        end_date: date = None,
        data_type: str = "weekly_exports",
        commodities: List[str] = None,
        **kwargs
    ) -> CollectorResult:
        """
        Fetch weekly export data from ANEC.

        Args:
            start_date: Start date for data range (defaults to start of ANEC data)
            end_date: End date (defaults to current date)
            data_type: 'weekly_exports' (default), 'all_history', or 'latest'
            commodities: Commodities to include (default: all)

        Returns:
            CollectorResult with fetched data
        """
        commodities = commodities or self.config.commodities

        if data_type == "weekly_exports":
            return self._fetch_weekly_exports(
                start_date=start_date,
                end_date=end_date,
                commodities=commodities,
                **kwargs
            )
        elif data_type == "all_history":
            return self._fetch_all_history(
                commodities=commodities,
                **kwargs
            )
        elif data_type == "latest":
            return self._fetch_latest_report(
                commodities=commodities,
                **kwargs
            )
        else:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"Unknown data_type: {data_type}"
            )

    # =========================================================================
    # WEEKLY EXPORT FETCHING
    # =========================================================================

    def _fetch_weekly_exports(
        self,
        start_date: date = None,
        end_date: date = None,
        commodities: List[str] = None,
        **kwargs
    ) -> CollectorResult:
        """
        Fetch weekly export reports for a given date range.

        Iterates through each ISO week in the range and attempts to download
        the corresponding ANEC report.
        """
        all_records = []
        warnings = []

        # Default date range
        if not end_date:
            end_date = date.today()
        if not start_date:
            start_date = date(end_date.year, 1, 1)

        # Generate list of (year, week) tuples for the date range
        weeks = self._get_weeks_in_range(start_date, end_date)

        logger.info(
            f"Fetching ANEC reports for {len(weeks)} weeks "
            f"({start_date} to {end_date})"
        )

        for year, week_num in weeks:
            records, week_warnings = self._fetch_single_week(
                year, week_num, commodities
            )
            all_records.extend(records)
            warnings.extend(week_warnings)

        if PANDAS_AVAILABLE and all_records:
            result_df = pd.DataFrame(all_records)
        else:
            result_df = all_records

        return CollectorResult(
            success=len(all_records) > 0,
            source=self.config.source_name,
            records_fetched=len(all_records),
            data=result_df,
            warnings=warnings,
            period_start=str(start_date),
            period_end=str(end_date),
        )

    def _fetch_all_history(
        self,
        commodities: List[str] = None,
        **kwargs
    ) -> CollectorResult:
        """
        Fetch all available historical ANEC reports from the first available
        year through the current date.
        """
        start_date = date(self.config.start_year, 1, 1)
        end_date = date.today()

        logger.info(
            f"Fetching full ANEC history from {self.config.start_year} to {end_date.year}"
        )

        return self._fetch_weekly_exports(
            start_date=start_date,
            end_date=end_date,
            commodities=commodities,
            **kwargs
        )

    def _fetch_latest_report(
        self,
        commodities: List[str] = None,
        **kwargs
    ) -> CollectorResult:
        """
        Fetch only the most recent available ANEC report.

        Tries the current week first, then walks backward up to 4 weeks.
        """
        today = date.today()
        iso_cal = today.isocalendar()
        year = iso_cal[0]
        week = iso_cal[1]

        all_records = []
        warnings = []

        # Try current week and up to 4 weeks back
        for offset in range(5):
            try_date = today - timedelta(weeks=offset)
            try_iso = try_date.isocalendar()
            try_year = try_iso[0]
            try_week = try_iso[1]

            records, week_warnings = self._fetch_single_week(
                try_year, try_week, commodities
            )

            if records:
                all_records.extend(records)
                logger.info(
                    f"Found latest ANEC report: Week {try_week}/{try_year}"
                )
                break

            warnings.extend(week_warnings)

        if not all_records:
            warnings.append("Could not find any recent ANEC report (checked last 5 weeks)")

        if PANDAS_AVAILABLE and all_records:
            result_df = pd.DataFrame(all_records)
        else:
            result_df = all_records

        return CollectorResult(
            success=len(all_records) > 0,
            source=self.config.source_name,
            records_fetched=len(all_records),
            data=result_df,
            warnings=warnings,
        )

    # =========================================================================
    # SINGLE WEEK PROCESSING
    # =========================================================================

    def _fetch_single_week(
        self,
        year: int,
        week_number: int,
        commodities: List[str] = None
    ) -> Tuple[List[Dict], List[str]]:
        """
        Fetch and parse a single week's ANEC report.

        Returns:
            Tuple of (records, warnings)
        """
        records = []
        warnings = []

        url = self._build_article_url(week_number, year)
        logger.debug(f"Fetching ANEC report: Week {week_number:02d}/{year} -> {url}")

        # Fetch the article page
        response, error = self._make_request(url)

        if error:
            warnings.append(f"Week {week_number:02d}/{year}: {error}")
            return records, warnings

        if response.status_code == 404:
            # Report doesn't exist for this week - not an error, just skip
            logger.debug(f"No report for Week {week_number:02d}/{year} (404)")
            return records, warnings

        if response.status_code == 403:
            warnings.append(
                f"Week {week_number:02d}/{year}: Access denied (403). "
                "ANEC may require browser access."
            )
            return records, warnings

        if response.status_code != 200:
            warnings.append(
                f"Week {week_number:02d}/{year}: HTTP {response.status_code}"
            )
            return records, warnings

        html_content = response.text

        # Strategy 1: Try to find and download PDF from the article page
        if self.config.download_pdfs:
            pdf_records = self._try_pdf_extraction(
                html_content, url, year, week_number, commodities
            )
            if pdf_records:
                records.extend(pdf_records)
                return records, warnings

        # Strategy 2: Parse data from HTML content directly
        html_records = self._parse_article_html(
            html_content, year, week_number, commodities
        )
        if html_records:
            records.extend(html_records)
            return records, warnings

        # Strategy 3: Extract any structured data from the page text
        text_records = self._extract_from_text(
            html_content, year, week_number, commodities
        )
        if text_records:
            records.extend(text_records)
        else:
            warnings.append(
                f"Week {week_number:02d}/{year}: Page found but could not "
                "extract structured data"
            )

        return records, warnings

    # =========================================================================
    # URL BUILDING
    # =========================================================================

    def _build_article_url(self, week_number: int, year: int) -> str:
        """
        Build the ANEC article URL for a specific week and year.

        Format: https://anec.com.br/article/anec-exportacoes-acumuladas-{WW}{YYYY}
        Example: https://anec.com.br/article/anec-exportacoes-acumuladas-042026
        """
        return (
            f"{self.config.article_base}/"
            f"anec-exportacoes-acumuladas-{week_number:02d}{year}"
        )

    # =========================================================================
    # PDF EXTRACTION
    # =========================================================================

    def _try_pdf_extraction(
        self,
        html_content: str,
        page_url: str,
        year: int,
        week_number: int,
        commodities: List[str] = None
    ) -> List[Dict]:
        """
        Try to find a PDF link in the article page, download it, and parse it.
        """
        records = []

        if not BS4_AVAILABLE:
            return records

        soup = BeautifulSoup(html_content, 'html.parser')

        # Find PDF links - look for common patterns
        pdf_links = self._find_pdf_links(soup, page_url)

        if not pdf_links:
            return records

        for pdf_url in pdf_links:
            # Download PDF
            pdf_path = self._download_pdf(pdf_url, year, week_number)
            if not pdf_path:
                continue

            # Parse PDF
            pdf_records = self._parse_pdf(pdf_path, year, week_number, commodities)
            records.extend(pdf_records)

        return records

    def _find_pdf_links(self, soup: BeautifulSoup, page_url: str) -> List[str]:
        """
        Find PDF download links in the article page.

        Looks for:
        - Direct <a href="*.pdf"> links
        - Embedded PDF viewers / iframes
        - JavaScript download triggers
        - Links with PDF-related text
        """
        pdf_urls = []

        # 1. Direct PDF links
        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.lower().endswith('.pdf'):
                full_url = self._resolve_url(href)
                if full_url not in pdf_urls:
                    pdf_urls.append(full_url)

        # 2. Links with PDF-related text or classes
        pdf_text_patterns = [
            r'download', r'pdf', r'relatório', r'relatorio',
            r'exporta', r'acumulad', r'documento'
        ]
        for link in soup.find_all('a', href=True):
            link_text = link.get_text(strip=True).lower()
            link_class = ' '.join(link.get('class', [])).lower()
            href = link['href'].lower()

            for pattern in pdf_text_patterns:
                if (re.search(pattern, link_text, re.I)
                        or re.search(pattern, link_class, re.I)
                        or re.search(pattern, href, re.I)):
                    full_url = self._resolve_url(link['href'])
                    if full_url not in pdf_urls and not full_url.endswith(('.html', '.htm')):
                        pdf_urls.append(full_url)
                    break

        # 3. Iframes that might embed PDFs
        for iframe in soup.find_all('iframe', src=True):
            src = iframe['src']
            if '.pdf' in src.lower() or 'viewer' in src.lower():
                full_url = self._resolve_url(src)
                if full_url not in pdf_urls:
                    pdf_urls.append(full_url)

        # 4. Embedded object/embed tags
        for embed in soup.find_all(['object', 'embed'], attrs={'data': True}):
            data = embed.get('data', '') or embed.get('src', '')
            if data and '.pdf' in data.lower():
                full_url = self._resolve_url(data)
                if full_url not in pdf_urls:
                    pdf_urls.append(full_url)

        return pdf_urls

    def _resolve_url(self, href: str) -> str:
        """Resolve a relative URL to an absolute URL."""
        if href.startswith(('http://', 'https://')):
            return href
        if href.startswith('//'):
            return f'https:{href}'
        if href.startswith('/'):
            return f"{self.config.source_url}{href}"
        return f"{self.config.source_url}/{href}"

    def _download_pdf(
        self,
        pdf_url: str,
        year: int,
        week_number: int
    ) -> Optional[Path]:
        """
        Download a PDF and cache it locally.

        Returns path to cached PDF, or None if download failed.
        """
        cache_filename = f"anec_exports_w{week_number:02d}_{year}.pdf"
        cache_path = self.config.pdf_cache_dir / cache_filename

        # Check if already cached
        if cache_path.exists() and cache_path.stat().st_size > 0:
            logger.debug(f"Using cached PDF: {cache_path}")
            return cache_path

        logger.info(f"Downloading ANEC PDF: {pdf_url}")

        response, error = self._make_request(pdf_url)

        if error:
            logger.warning(f"PDF download failed: {error}")
            return None

        if response.status_code != 200:
            logger.warning(f"PDF download HTTP {response.status_code}: {pdf_url}")
            return None

        content_type = response.headers.get('Content-Type', '')
        if 'pdf' not in content_type.lower() and 'octet-stream' not in content_type.lower():
            # Might not be a PDF - check magic bytes
            if not response.content[:5] == b'%PDF-':
                logger.warning(f"Response is not a PDF (Content-Type: {content_type})")
                return None

        try:
            with open(cache_path, 'wb') as f:
                f.write(response.content)
            logger.info(f"PDF saved: {cache_path} ({len(response.content)} bytes)")
            return cache_path
        except OSError as e:
            logger.error(f"Failed to save PDF: {e}")
            return None

    def _parse_pdf(
        self,
        pdf_path: Path,
        year: int,
        week_number: int,
        commodities: List[str] = None
    ) -> List[Dict]:
        """
        Parse an ANEC PDF report to extract weekly export data.

        The typical ANEC PDF contains tables with columns like:
        - Product/Commodity
        - Weekly accumulated volume (tonnes)
        - Monthly accumulated volume (tonnes)
        - Year-to-date (YTD) volume
        - Comparison with prior year
        """
        records = []
        commodities = commodities or list(ANEC_COMMODITIES.values())
        commodity_en_names = [c if isinstance(c, str) else c.get('en_name', '') for c in commodities]

        if not PDFPLUMBER_AVAILABLE:
            logger.warning(
                "pdfplumber not available - install with: pip install pdfplumber"
            )
            return self._parse_pdf_fallback(pdf_path, year, week_number, commodity_en_names)

        try:
            with pdfplumber.open(str(pdf_path)) as pdf:
                for page_idx, page in enumerate(pdf.pages):
                    # Extract tables from the page
                    tables = page.extract_tables()

                    for table in tables:
                        table_records = self._parse_export_table(
                            table, year, week_number, commodity_en_names, page_idx
                        )
                        records.extend(table_records)

                    # If no tables found, try text extraction
                    if not tables:
                        text = page.extract_text()
                        if text:
                            text_records = self._parse_export_text(
                                text, year, week_number, commodity_en_names
                            )
                            records.extend(text_records)

        except Exception as e:
            logger.error(f"Error parsing PDF {pdf_path}: {e}")
            return self._parse_pdf_fallback(pdf_path, year, week_number, commodity_en_names)

        return records

    def _parse_export_table(
        self,
        table: List[List[str]],
        year: int,
        week_number: int,
        commodities: List[str],
        page_idx: int = 0
    ) -> List[Dict]:
        """
        Parse a single table from the PDF.

        ANEC tables typically have:
        - Row headers identifying the commodity or product
        - Columns for different time periods (week, month, YTD)
        - Values in tonnes
        """
        records = []

        if not table or len(table) < 2:
            return records

        # Try to identify headers from the first row(s)
        headers = []
        data_start_row = 0

        for i, row in enumerate(table[:3]):
            if row and any(
                cell and any(
                    kw in str(cell).lower()
                    for kw in ['produto', 'product', 'acumulado', 'accumulated',
                              'semana', 'week', 'mês', 'month', 'ano', 'year',
                              'toneladas', 'tonnes', 'total']
                )
                for cell in row if cell
            ):
                headers = [str(cell).strip() if cell else '' for cell in row]
                data_start_row = i + 1
                break

        if not headers:
            # Use first row as headers
            headers = [str(cell).strip() if cell else f'col_{j}' for j, cell in enumerate(table[0])]
            data_start_row = 1

        # Parse data rows
        for row in table[data_start_row:]:
            if not row or all(cell is None or str(cell).strip() == '' for cell in row):
                continue

            # Try to identify which commodity this row represents
            row_text = ' '.join(str(cell).lower() for cell in row if cell)
            commodity = self._identify_commodity(row_text)

            if not commodity:
                # Check if the row matches a commodity we're tracking
                continue

            if commodity not in commodities:
                continue

            record = {
                'source': 'ANEC',
                'commodity': commodity,
                'year': year,
                'week_number': week_number,
                'report_url': self._build_article_url(week_number, year),
                'page_index': page_idx,
                'collected_at': datetime.now().isoformat(),
            }

            # Extract values from the row
            for j, cell in enumerate(row):
                if cell is None or str(cell).strip() == '':
                    continue

                cell_str = str(cell).strip()
                header = headers[j] if j < len(headers) else f'col_{j}'
                header_lower = header.lower()

                # Try to parse numeric values
                numeric_val = self._safe_float(cell_str)

                if numeric_val is not None:
                    # Map to appropriate field based on header
                    if any(kw in header_lower for kw in ['semana', 'week', 'semanal']):
                        record['weekly_volume_tonnes'] = numeric_val
                    elif any(kw in header_lower for kw in ['mês', 'month', 'mensal']):
                        record['monthly_volume_tonnes'] = numeric_val
                    elif any(kw in header_lower for kw in ['acumulado', 'accumulated', 'ytd', 'ano']):
                        # Check if it's a comparison year
                        year_match = re.search(r'20\d{2}', header)
                        if year_match:
                            comp_year = int(year_match.group())
                            record[f'ytd_{comp_year}_tonnes'] = numeric_val
                        else:
                            record['ytd_volume_tonnes'] = numeric_val
                    elif any(kw in header_lower for kw in ['total']):
                        record['total_volume_tonnes'] = numeric_val
                    elif any(kw in header_lower for kw in ['variação', 'variacao', 'var', 'change', '%']):
                        record['yoy_change_pct'] = numeric_val
                    else:
                        # Store as generic numbered column
                        record[f'value_{j}'] = numeric_val

            # Only add if we have some volume data
            has_volume = any(
                k for k in record
                if 'volume' in k or 'ytd' in k or 'total' in k or k.startswith('value_')
            )
            if has_volume:
                records.append(record)

        return records

    def _parse_export_text(
        self,
        text: str,
        year: int,
        week_number: int,
        commodities: List[str]
    ) -> List[Dict]:
        """
        Fallback: parse export data from PDF text when tables aren't detected.

        Looks for patterns like:
        - "Soja: 1.234.567 toneladas"
        - "Farelo de Soja ... 456.789 t"
        - "Exportações de milho: 234.567 tonnes"
        """
        records = []

        # Split text into lines for analysis
        lines = text.split('\n')

        for line in lines:
            line_lower = line.lower().strip()
            if not line_lower:
                continue

            # Identify commodity in the line
            commodity = self._identify_commodity(line_lower)
            if not commodity or commodity not in commodities:
                continue

            # Find numeric values (Brazilian format: 1.234.567,89)
            # Pattern matches numbers with dots as thousands separators
            number_patterns = re.findall(
                r'([\d]{1,3}(?:\.[\d]{3})*(?:,\d+)?)\s*(?:t(?:on)?(?:eladas)?|tonnes?)?',
                line
            )

            for num_str in number_patterns:
                value = self._safe_float(num_str)
                if value and value > 100:  # Filter out small numbers (likely not tonnes)
                    record = {
                        'source': 'ANEC',
                        'commodity': commodity,
                        'year': year,
                        'week_number': week_number,
                        'volume_tonnes': value,
                        'raw_text': line.strip()[:200],
                        'report_url': self._build_article_url(week_number, year),
                        'extraction_method': 'text_regex',
                        'collected_at': datetime.now().isoformat(),
                    }
                    records.append(record)
                    break  # One value per line per commodity

        return records

    def _parse_pdf_fallback(
        self,
        pdf_path: Path,
        year: int,
        week_number: int,
        commodities: List[str]
    ) -> List[Dict]:
        """
        Minimal fallback when pdfplumber is not available.
        Records the PDF location for manual processing.
        """
        return [{
            'source': 'ANEC',
            'year': year,
            'week_number': week_number,
            'pdf_path': str(pdf_path),
            'report_url': self._build_article_url(week_number, year),
            'status': 'pdf_downloaded_parse_pending',
            'note': 'Install pdfplumber for automatic parsing: pip install pdfplumber',
            'collected_at': datetime.now().isoformat(),
        }]

    # =========================================================================
    # HTML PARSING (when PDF is not available)
    # =========================================================================

    def _parse_article_html(
        self,
        html_content: str,
        year: int,
        week_number: int,
        commodities: List[str] = None
    ) -> List[Dict]:
        """
        Parse export data directly from the article HTML.

        Some ANEC articles embed the data as HTML tables rather than
        linking to a separate PDF.
        """
        records = []

        if not BS4_AVAILABLE:
            return records

        soup = BeautifulSoup(html_content, 'html.parser')

        # Look for data tables in the article body
        article_body = (
            soup.find('article')
            or soup.find('div', class_=re.compile(r'article|content|body|post', re.I))
            or soup
        )

        tables = article_body.find_all('table')

        for table in tables:
            table_records = self._parse_html_table(
                table, year, week_number, commodities
            )
            records.extend(table_records)

        return records

    def _parse_html_table(
        self,
        table,
        year: int,
        week_number: int,
        commodities: List[str] = None
    ) -> List[Dict]:
        """Parse an HTML table for export data."""
        records = []

        try:
            rows = table.find_all('tr')
            if len(rows) < 2:
                return records

            # Extract headers
            header_row = rows[0]
            headers = [
                cell.get_text(strip=True).lower()
                for cell in header_row.find_all(['th', 'td'])
            ]

            # Check if this looks like an export data table
            export_keywords = [
                'produto', 'product', 'commodity',
                'toneladas', 'tonnes', 'volume',
                'acumulado', 'accumulated', 'total',
                'soja', 'milho', 'trigo', 'farelo',
            ]
            header_text = ' '.join(headers)
            if not any(kw in header_text for kw in export_keywords):
                return records

            # Parse data rows
            for row in rows[1:]:
                cells = row.find_all(['td', 'th'])
                if len(cells) != len(headers):
                    continue

                values = [cell.get_text(strip=True) for cell in cells]
                row_text = ' '.join(v.lower() for v in values)

                commodity = self._identify_commodity(row_text)
                if not commodity:
                    continue

                if commodities and commodity not in commodities:
                    continue

                record = {
                    'source': 'ANEC',
                    'commodity': commodity,
                    'year': year,
                    'week_number': week_number,
                    'report_url': self._build_article_url(week_number, year),
                    'extraction_method': 'html_table',
                    'collected_at': datetime.now().isoformat(),
                }

                for header, value in zip(headers, values):
                    numeric_val = self._safe_float(value)
                    if numeric_val is not None:
                        if any(kw in header for kw in ['semana', 'week']):
                            record['weekly_volume_tonnes'] = numeric_val
                        elif any(kw in header for kw in ['mês', 'month']):
                            record['monthly_volume_tonnes'] = numeric_val
                        elif any(kw in header for kw in ['acumulado', 'ytd', 'ano']):
                            year_match = re.search(r'20\d{2}', header)
                            if year_match:
                                record[f'ytd_{year_match.group()}_tonnes'] = numeric_val
                            else:
                                record['ytd_volume_tonnes'] = numeric_val
                        elif any(kw in header for kw in ['total']):
                            record['total_volume_tonnes'] = numeric_val

                has_volume = any(
                    k for k in record
                    if 'volume' in k or 'ytd' in k or 'total' in k
                )
                if has_volume:
                    records.append(record)

        except Exception as e:
            logger.warning(f"Error parsing HTML table: {e}")

        return records

    def _extract_from_text(
        self,
        html_content: str,
        year: int,
        week_number: int,
        commodities: List[str] = None
    ) -> List[Dict]:
        """
        Last resort: extract structured data from article text content.
        """
        records = []

        if BS4_AVAILABLE:
            soup = BeautifulSoup(html_content, 'html.parser')
            text = soup.get_text(separator='\n')
        else:
            # Simple tag stripping
            text = re.sub(r'<[^>]+>', '\n', html_content)

        return self._parse_export_text(text, year, week_number, commodities or self.config.commodities)

    # =========================================================================
    # HELPERS
    # =========================================================================

    def _get_weeks_in_range(
        self,
        start_date: date,
        end_date: date
    ) -> List[Tuple[int, int]]:
        """
        Get list of (year, week_number) tuples for all ISO weeks in a date range.
        """
        weeks = []
        current = start_date

        while current <= end_date:
            iso_cal = current.isocalendar()
            year_week = (iso_cal[0], iso_cal[1])
            if year_week not in weeks:
                weeks.append(year_week)
            current += timedelta(days=7)

        # Make sure we include the end date's week
        end_iso = end_date.isocalendar()
        end_week = (end_iso[0], end_iso[1])
        if end_week not in weeks:
            weeks.append(end_week)

        return weeks

    def _identify_commodity(self, text: str) -> Optional[str]:
        """
        Identify which commodity a text refers to.

        Returns the English commodity name or None.
        """
        text_lower = text.lower()

        # Check for soybean meal FIRST (before soybeans, since "soja" is in both)
        for keyword in COMMODITY_KEYWORDS['soybean_meal']:
            if keyword in text_lower:
                return 'soybean_meal'

        # Then check soybeans
        for keyword in COMMODITY_KEYWORDS['soybeans']:
            if keyword in text_lower:
                return 'soybeans'

        # Corn
        for keyword in COMMODITY_KEYWORDS['corn']:
            if keyword in text_lower:
                return 'corn'

        # Wheat
        for keyword in COMMODITY_KEYWORDS['wheat']:
            if keyword in text_lower:
                return 'wheat'

        return None

    def _safe_float(self, value: Any) -> Optional[float]:
        """
        Safely convert value to float, handling Brazilian number format.

        Brazilian format: 1.234.567,89
        - Dots are thousands separators
        - Comma is the decimal separator
        """
        if value is None or str(value).strip() == '':
            return None
        try:
            str_val = str(value).strip()
            # Remove currency symbols and percentage signs
            str_val = re.sub(r'[R$%]', '', str_val).strip()
            # Remove spaces (sometimes used as thousands separator)
            str_val = str_val.replace(' ', '')
            # Handle Brazilian format: replace dots (thousands) then comma (decimal)
            str_val = str_val.replace('.', '').replace(',', '.')
            result = float(str_val)
            return result
        except (ValueError, TypeError):
            return None

    def parse_response(self, response_data: Any) -> Any:
        """Parse API response (required by BaseCollector)."""
        return response_data

    # =========================================================================
    # CONVENIENCE METHODS
    # =========================================================================

    def get_latest_exports(self) -> Optional[Any]:
        """Get the most recent weekly export report."""
        result = self.collect(data_type="latest")
        return result.data if result.success else None

    def get_weekly_exports(
        self,
        year: int = None,
        week: int = None
    ) -> Optional[Any]:
        """Get export data for a specific week."""
        if not year or not week:
            return self.get_latest_exports()

        # Calculate date range for the specific week
        # ISO week to date
        jan4 = date(year, 1, 4)
        start_of_year = jan4 - timedelta(days=jan4.isoweekday() - 1)
        week_start = start_of_year + timedelta(weeks=week - 1)
        week_end = week_start + timedelta(days=6)

        result = self.collect(
            data_type="weekly_exports",
            start_date=week_start,
            end_date=week_end,
        )
        return result.data if result.success else None

    def get_ytd_comparison(self, year: int = None) -> Optional[Any]:
        """
        Get year-to-date export data for comparison with prior year.
        """
        year = year or date.today().year
        start = date(year, 1, 1)
        end = date.today() if year == date.today().year else date(year, 12, 31)

        result = self.collect(
            data_type="weekly_exports",
            start_date=start,
            end_date=end,
        )
        return result.data if result.success else None

    def discover_available_reports(
        self,
        start_year: int = None,
        end_year: int = None
    ) -> List[Dict[str, Any]]:
        """
        Discover which weekly reports are available on the ANEC website.

        Returns a list of dicts with year, week_number, url, and http_status.
        """
        start_year = start_year or self.config.start_year
        end_year = end_year or date.today().year
        available = []

        for year in range(start_year, end_year + 1):
            max_week = 52 if year < date.today().year else date.today().isocalendar()[1]

            for week_num in range(1, max_week + 1):
                url = self._build_article_url(week_num, year)
                response, error = self._make_request(url, method="HEAD")

                status = 'error'
                if error:
                    status = 'error'
                elif response.status_code == 200:
                    status = 'available'
                elif response.status_code == 404:
                    status = 'not_found'
                elif response.status_code == 403:
                    status = 'forbidden'
                else:
                    status = f'http_{response.status_code}'

                entry = {
                    'year': year,
                    'week_number': week_num,
                    'url': url,
                    'status': status,
                }

                if status == 'available':
                    available.append(entry)
                    logger.info(f"Found: Week {week_num:02d}/{year}")
                else:
                    logger.debug(f"Not found: Week {week_num:02d}/{year} ({status})")

        return available


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    """Command-line interface for ANEC collector"""
    import argparse
    import json

    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(
        description='ANEC Weekly Exports Collector'
    )

    parser.add_argument(
        'command',
        choices=['latest', 'weekly', 'history', 'discover', 'test'],
        help='Command to execute'
    )

    parser.add_argument(
        '--year', '-y',
        type=int,
        help='Year for weekly/history commands'
    )

    parser.add_argument(
        '--week', '-w',
        type=int,
        help='ISO week number for weekly command'
    )

    parser.add_argument(
        '--start-date',
        type=str,
        help='Start date (YYYY-MM-DD) for history command'
    )

    parser.add_argument(
        '--end-date',
        type=str,
        help='End date (YYYY-MM-DD) for history command'
    )

    parser.add_argument(
        '--commodities',
        nargs='+',
        default=['soybeans', 'soybean_meal', 'corn', 'wheat'],
        help='Commodities to fetch'
    )

    parser.add_argument(
        '--output', '-o',
        help='Output file (JSON or CSV)'
    )

    parser.add_argument(
        '--no-pdf',
        action='store_true',
        help='Skip PDF downloading (HTML parsing only)'
    )

    args = parser.parse_args()

    config = ANECConfig(
        commodities=args.commodities,
        download_pdfs=not args.no_pdf,
    )
    collector = ANECCollector(config)

    if args.command == 'test':
        success, message = collector.test_connection()
        print(f"Connection test: {'PASS' if success else 'FAIL'} - {message}")
        return

    if args.command == 'discover':
        available = collector.discover_available_reports(
            start_year=args.year or ANEC_FIRST_YEAR
        )
        print(f"Found {len(available)} available reports")
        for entry in available:
            print(f"  Week {entry['week_number']:02d}/{entry['year']}: {entry['url']}")
        return

    if args.command == 'latest':
        result = collector.collect(
            data_type="latest",
            commodities=args.commodities,
        )
    elif args.command == 'weekly':
        if not args.year or not args.week:
            print("Error: --year and --week required for weekly command")
            return
        data = collector.get_weekly_exports(args.year, args.week)
        result = CollectorResult(
            success=data is not None,
            source='ANEC',
            data=data,
            records_fetched=len(data) if data is not None else 0,
        )
    elif args.command == 'history':
        start = None
        end = None
        if args.start_date:
            start = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        if args.end_date:
            end = datetime.strptime(args.end_date, '%Y-%m-%d').date()

        result = collector.collect(
            data_type="all_history" if not start else "weekly_exports",
            start_date=start,
            end_date=end,
            commodities=args.commodities,
        )
    else:
        return

    print(f"\nSuccess: {result.success}")
    print(f"Records: {result.records_fetched}")

    if result.warnings:
        print(f"Warnings ({len(result.warnings)}):")
        for w in result.warnings[:10]:
            print(f"  - {w}")
        if len(result.warnings) > 10:
            print(f"  ... and {len(result.warnings) - 10} more")

    if result.error_message:
        print(f"Error: {result.error_message}")

    if args.output and result.data is not None:
        if args.output.endswith('.csv') and PANDAS_AVAILABLE:
            if hasattr(result.data, 'to_csv'):
                result.data.to_csv(args.output, index=False)
            else:
                pd.DataFrame(result.data).to_csv(args.output, index=False)
        else:
            if PANDAS_AVAILABLE and hasattr(result.data, 'to_json'):
                result.data.to_json(args.output, orient='records', date_format='iso')
            else:
                with open(args.output, 'w') as f:
                    json.dump(
                        result.data if isinstance(result.data, list) else [result.data],
                        f, indent=2, default=str
                    )
        print(f"Saved to: {args.output}")
    elif result.data is not None:
        print("\nData:")
        if PANDAS_AVAILABLE and hasattr(result.data, 'head'):
            print(result.data.head(10))
        else:
            data_list = result.data if isinstance(result.data, list) else [result.data]
            print(json.dumps(data_list[:5], indent=2, default=str))


if __name__ == '__main__':
    main()

"""
WASDE Analysis Template

Concrete implementation of BaseAnalysisTemplate for the monthly USDA WASDE
report. Gathers US and global balance sheet data, computes MoM deltas,
and builds prompt variables for the WASDeAnalysisV1 prompt template.
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional

from src.analysis.templates.base_template import BaseAnalysisTemplate

logger = logging.getLogger(__name__)

# S&D line items to include in the balance sheet table
_BALANCE_ITEMS = [
    ('production', 'Production'),
    ('total_supply', 'Total Supply'),
    ('feed_dom_consumption', 'Feed & Residual'),
    ('fsi_consumption', 'FSI'),
    ('domestic_consumption', 'Total Domestic'),
    ('exports', 'Exports'),
    ('ending_stocks', 'Ending Stocks'),
]


class WASDeAnalysisTemplate(BaseAnalysisTemplate):
    """WASDE monthly balance sheet analysis template."""

    template_id = 'wasde_monthly'
    report_type = 'wasde'
    prompt_template_id = 'wasde_analysis_v1'
    required_collectors = ['usda_wasde']
    trigger_mode = 'event'
    trigger_collector = 'usda_wasde'
    kg_node_keys = [
        'usda.wasde.revision_pattern',
        'august_wasde_pivot',
        'corn',
        'soybeans',
        'wheat',
        'crop_condition_yield_model',
    ]

    _COMMODITIES = ['corn', 'soybeans', 'wheat']
    _GLOBAL_COUNTRIES = [
        ('BR', 'Brazil'),
        ('AR', 'Argentina'),
        ('CH', 'China'),
    ]

    # ------------------------------------------------------------------
    # Abstract method implementations
    # ------------------------------------------------------------------

    def check_data_ready(self) -> bool:
        """Check if US balance sheet data exists for corn/soy/wheat."""
        try:
            with self._get_connection() as conn:
                cur = conn.cursor()
                cur.execute("""
                    SELECT COUNT(DISTINCT commodity) as cnt
                    FROM bronze.fas_psd
                    WHERE country_code = 'US'
                      AND commodity IN ('corn', 'soybeans', 'wheat')
                      AND ending_stocks IS NOT NULL
                      AND report_date = (
                          SELECT MAX(report_date) FROM bronze.fas_psd
                          WHERE country_code = 'US'
                            AND commodity IN ('corn', 'soybeans', 'wheat')
                      )
                """)
                row = cur.fetchone()
                return row is not None and int(row['cnt']) >= 1
        except Exception as e:
            logger.debug("check_data_ready failed: %s", e)
            return False

    def gather_data(self) -> Dict:
        """
        Query US and global balance sheet data from bronze.fas_psd.

        Returns dict with keys:
          us_balance_sheets, global_sd, report_date, prior_date, marketing_year
        """
        with self._get_connection() as conn:
            cur = conn.cursor()

            # US balance sheets: two most recent report_date per commodity
            cur.execute("""
                WITH ranked AS (
                    SELECT commodity, marketing_year, report_date,
                           production, total_supply, beginning_stocks,
                           imports, domestic_consumption, feed_dom_consumption,
                           fsi_consumption, exports, ending_stocks, crush,
                           ROW_NUMBER() OVER (
                               PARTITION BY commodity
                               ORDER BY report_date DESC, marketing_year DESC
                           ) as rn
                    FROM bronze.fas_psd
                    WHERE country_code = 'US'
                      AND commodity IN ('corn', 'soybeans', 'wheat')
                      AND ending_stocks IS NOT NULL
                )
                SELECT r1.commodity, r1.marketing_year, r1.rn,
                       r1.report_date, r1.production, r1.total_supply,
                       r1.beginning_stocks, r1.imports,
                       r1.domestic_consumption, r1.feed_dom_consumption,
                       r1.fsi_consumption, r1.exports, r1.ending_stocks,
                       r1.crush,
                       CASE WHEN (COALESCE(r1.domestic_consumption,0)
                                + COALESCE(r1.exports,0)) > 0
                            THEN ROUND(r1.ending_stocks
                                 / (r1.domestic_consumption + r1.exports)
                                 * 100, 1)
                            ELSE NULL
                       END as stocks_use_pct
                FROM ranked r1
                WHERE r1.rn <= 2
                ORDER BY r1.commodity, r1.rn
            """)
            us_rows = cur.fetchall()

            # Parse into structured dict
            us_balance_sheets = {}
            report_date = None
            prior_date = None
            marketing_year = None

            for row in us_rows:
                commodity = row['commodity']
                rn = int(row['rn'])
                if commodity not in us_balance_sheets:
                    us_balance_sheets[commodity] = {}
                period = 'current' if rn == 1 else 'prior'
                us_balance_sheets[commodity][period] = dict(row)

                if rn == 1 and report_date is None:
                    report_date = str(row['report_date'])
                    marketing_year = int(row['marketing_year']) if row['marketing_year'] is not None else None
                if rn == 2 and prior_date is None:
                    prior_date = str(row['report_date'])

            # Global S&D: Brazil, Argentina, China (two most recent)
            cur.execute("""
                WITH ranked AS (
                    SELECT commodity, country_code, marketing_year, report_date,
                           production, exports, ending_stocks,
                           domestic_consumption,
                           ROW_NUMBER() OVER (
                               PARTITION BY commodity, country_code
                               ORDER BY report_date DESC, marketing_year DESC
                           ) as rn
                    FROM bronze.fas_psd
                    WHERE country_code IN ('BR', 'AR', 'CH')
                      AND commodity IN ('corn', 'soybeans', 'wheat')
                      AND ending_stocks IS NOT NULL
                )
                SELECT commodity, country_code, rn, report_date,
                       production, exports, ending_stocks, domestic_consumption
                FROM ranked
                WHERE rn <= 2
                ORDER BY country_code, commodity, rn
            """)
            global_rows = cur.fetchall()

            global_sd = {}
            for row in global_rows:
                key = f"{row['country_code']}_{row['commodity']}"
                rn = int(row['rn'])
                if key not in global_sd:
                    global_sd[key] = {}
                period = 'current' if rn == 1 else 'prior'
                global_sd[key][period] = dict(row)

        return {
            'us_balance_sheets': us_balance_sheets,
            'global_sd': global_sd,
            'report_date': report_date,
            'prior_date': prior_date,
            'marketing_year': marketing_year,
        }

    def compute_analysis(self, data: Dict) -> Dict:
        """
        Transform gathered data into prompt template variables.

        Returns dict with keys matching WASDeAnalysisV1.REQUIRED_VARIABLES.
        """
        us = data['us_balance_sheets']
        report_date = data.get('report_date', 'Unknown')
        my = data.get('marketing_year')

        # Format marketing year as "2024/25"
        if my is not None:
            marketing_year = f"{my}/{str(my + 1)[-2:]}"
        else:
            marketing_year = 'Unknown'

        # August WASDE detection
        is_august = False
        try:
            if report_date and report_date != 'Unknown':
                is_august = datetime.strptime(report_date, '%Y-%m-%d').month == 8
        except (ValueError, TypeError):
            pass

        return {
            'report_date': report_date,
            'marketing_year': marketing_year,
            'balance_sheet_table': self._build_balance_sheet_table(us),
            'delta_summary': self._build_delta_summary(us),
            'global_context': self._build_global_context(data.get('global_sd', {})),
            'is_august_wasde': 'Yes' if is_august else 'No',
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_balance_sheet_table(self, us: Dict) -> str:
        """Build a markdown table: Item | Corn (cur/chg) | Soy (cur/chg) | Wheat (cur/chg)."""
        header = "| Item | Corn | Chg | Soybeans | Chg | Wheat | Chg |"
        sep = "|------|------|-----|----------|-----|-------|-----|"
        rows = [header, sep]

        for col, label in _BALANCE_ITEMS:
            parts = [f"| {label}"]
            for commodity in self._COMMODITIES:
                bs = us.get(commodity, {})
                cur_val = self._get_val(bs, 'current', col)
                pri_val = self._get_val(bs, 'prior', col)

                if cur_val is not None:
                    parts.append(f" {cur_val:,.0f}")
                else:
                    parts.append(" -")

                if cur_val is not None and pri_val is not None:
                    chg = cur_val - pri_val
                    sign = '+' if chg >= 0 else ''
                    parts.append(f" {sign}{chg:,.0f}")
                else:
                    parts.append(" -")
            parts.append("")
            rows.append(" |".join(parts))

        # Add stocks-to-use row
        parts = ["| Stocks/Use %"]
        for commodity in self._COMMODITIES:
            bs = us.get(commodity, {})
            cur_stu = self._get_val(bs, 'current', 'stocks_use_pct')
            pri_stu = self._get_val(bs, 'prior', 'stocks_use_pct')

            if cur_stu is not None:
                parts.append(f" {cur_stu:.1f}%")
            else:
                parts.append(" -")

            if cur_stu is not None and pri_stu is not None:
                chg = cur_stu - pri_stu
                sign = '+' if chg >= 0 else ''
                parts.append(f" {sign}{chg:.1f}")
            else:
                parts.append(" -")
        parts.append("")
        rows.append(" |".join(parts))

        return "\n".join(rows)

    def _build_delta_summary(self, us: Dict) -> str:
        """Build plain-English MoM change summary per commodity."""
        lines = []
        for commodity in self._COMMODITIES:
            bs = us.get(commodity, {})
            cur = bs.get('current', {})
            pri = bs.get('prior', {})
            if not cur or not pri:
                continue

            changes = []
            for col, label in [
                ('ending_stocks', 'ending stocks'),
                ('production', 'production'),
                ('exports', 'exports'),
            ]:
                c = self._safe_float(cur.get(col))
                p = self._safe_float(pri.get(col))
                if c is not None and p is not None and c != p:
                    diff = c - p
                    direction = 'raised' if diff > 0 else 'cut'
                    changes.append(f"{label} {direction} {abs(diff):,.0f}")

            if changes:
                lines.append(f"**{commodity.title()}**: {'; '.join(changes)}")
            else:
                lines.append(f"**{commodity.title()}**: No changes")

        return "\n".join(lines) if lines else "No month-over-month changes detected."

    def _build_global_context(self, global_sd: Dict) -> str:
        """Build global S&D summary for BR, AR, CN."""
        lines = []
        for code, name in self._GLOBAL_COUNTRIES:
            country_lines = []
            for commodity in self._COMMODITIES:
                key = f"{code}_{commodity}"
                entry = global_sd.get(key, {})
                cur = entry.get('current', {})
                pri = entry.get('prior', {})

                if not cur:
                    continue

                prod = self._safe_float(cur.get('production'))
                prod_pri = self._safe_float(pri.get('production')) if pri else None
                exp = self._safe_float(cur.get('exports'))
                exp_pri = self._safe_float(pri.get('exports')) if pri else None

                parts = []
                if prod is not None:
                    prod_str = f"prod {prod:,.0f}"
                    if prod_pri is not None and prod != prod_pri:
                        diff = prod - prod_pri
                        sign = '+' if diff >= 0 else ''
                        prod_str += f" ({sign}{diff:,.0f})"
                    parts.append(prod_str)

                if exp is not None:
                    exp_str = f"exp {exp:,.0f}"
                    if exp_pri is not None and exp != exp_pri:
                        diff = exp - exp_pri
                        sign = '+' if diff >= 0 else ''
                        exp_str += f" ({sign}{diff:,.0f})"
                    parts.append(exp_str)

                if parts:
                    country_lines.append(f"  - {commodity.title()}: {', '.join(parts)}")

            if country_lines:
                lines.append(f"**{name}:**")
                lines.extend(country_lines)

        return "\n".join(lines) if lines else "No global S&D data available."

    @staticmethod
    def _get_val(bs: Dict, period: str, col: str):
        """Safely extract a numeric value from the balance sheet dict."""
        entry = bs.get(period, {})
        val = entry.get(col)
        if val is None:
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _safe_float(val) -> Optional[float]:
        """Convert a value to float, returning None on failure."""
        if val is None:
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

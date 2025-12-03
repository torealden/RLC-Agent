"""
Trade Data Harmonization Utilities

Provides tools for:
- HS code normalization across countries
- Unit conversion (to metric tons and USD)
- Cross-country data balancing/reconciliation
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from decimal import Decimal

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


# =============================================================================
# HS CODE NORMALIZATION
# =============================================================================

class HSCodeNormalizer:
    """
    Normalize HS codes across different country formats

    Different countries use:
    - Argentina, Brazil, Uruguay, Paraguay: NCM (MERCOSUR) - 8 digits
    - Colombia: 10-digit subheadings
    - International: HS6 (6 digits) for comparability
    """

    def __init__(self, target_level: int = 6):
        """
        Initialize normalizer

        Args:
            target_level: Target HS code level for harmonization (default 6)
        """
        self.target_level = target_level

    def normalize(self, hs_code: str, source_level: int = None) -> str:
        """
        Normalize HS code to target level

        Args:
            hs_code: Original HS code
            source_level: Original code level (auto-detected if None)

        Returns:
            Normalized HS code string
        """
        if not hs_code:
            return ""

        # Clean the code
        clean_code = ''.join(c for c in str(hs_code) if c.isdigit())

        # Determine source level if not provided
        if source_level is None:
            source_level = len(clean_code)

        # Truncate or pad
        if len(clean_code) >= self.target_level:
            return clean_code[:self.target_level]
        else:
            # Pad with zeros (unusual but handle gracefully)
            return clean_code.ljust(self.target_level, '0')

    def get_chapter(self, hs_code: str) -> str:
        """Get HS chapter (first 2 digits)"""
        normalized = self.normalize(hs_code, source_level=None)
        return normalized[:2] if len(normalized) >= 2 else normalized

    def get_heading(self, hs_code: str) -> str:
        """Get HS heading (first 4 digits)"""
        normalized = self.normalize(hs_code, source_level=None)
        return normalized[:4] if len(normalized) >= 4 else normalized

    def get_subheading(self, hs_code: str) -> str:
        """Get HS subheading (first 6 digits)"""
        return self.normalize(hs_code)[:6]

    def is_valid_hs_code(self, hs_code: str) -> bool:
        """Check if HS code is valid"""
        if not hs_code:
            return False

        clean_code = ''.join(c for c in str(hs_code) if c.isdigit())

        # Must be at least 2 digits
        if len(clean_code) < 2:
            return False

        # Chapter must be 01-99
        chapter = int(clean_code[:2])
        if chapter < 1 or chapter > 99:
            return False

        return True


# =============================================================================
# UNIT CONVERSION
# =============================================================================

class UnitConverter:
    """Convert quantities and values to standard units"""

    # Weight conversion factors to metric tons
    WEIGHT_TO_TONS = {
        'kg': 0.001,
        'kilogram': 0.001,
        'kilogramo': 0.001,
        'ton': 1.0,
        'tons': 1.0,
        'tonne': 1.0,
        'metric ton': 1.0,
        'mt': 1.0,
        'lb': 0.000453592,
        'lbs': 0.000453592,
        'pound': 0.000453592,
        'oz': 0.0000283495,
        'ounce': 0.0000283495,
        'g': 0.000001,
        'gram': 0.000001,
    }

    # Currency conversion (placeholder - should use real-time rates)
    # These are approximate rates and should be updated from an API
    CURRENCY_TO_USD = {
        'USD': 1.0,
        'EUR': 1.08,
        'BRL': 0.20,  # Brazilian Real
        'ARS': 0.001,  # Argentine Peso (highly variable)
        'COP': 0.00025,  # Colombian Peso
        'UYU': 0.025,  # Uruguayan Peso
        'PYG': 0.00013,  # Paraguayan Guarani
    }

    @classmethod
    def to_metric_tons(cls, quantity: float, unit: str) -> Optional[float]:
        """Convert quantity to metric tons"""
        if quantity is None:
            return None

        unit_lower = str(unit).lower().strip() if unit else 'kg'
        factor = cls.WEIGHT_TO_TONS.get(unit_lower, 0.001)  # Default to kg

        return quantity * factor

    @classmethod
    def to_usd(cls, value: float, currency: str) -> Optional[float]:
        """
        Convert value to USD

        Note: For production use, integrate with a real-time FX API
        """
        if value is None:
            return None

        currency_upper = str(currency).upper().strip() if currency else 'USD'
        rate = cls.CURRENCY_TO_USD.get(currency_upper, 1.0)

        return value * rate


# =============================================================================
# TRADE DATA HARMONIZER
# =============================================================================

@dataclass
class HarmonizedRecord:
    """A harmonized trade flow record"""
    reporter_country: str
    partner_country: str
    flow: str
    period: str
    year: int
    month: Optional[int]
    hs_code_6: str
    chapter: str
    quantity_tons: Optional[float]
    value_usd: float
    source: str
    source_count: int = 1


class TradeDataHarmonizer:
    """
    Harmonize trade data from multiple sources

    Steps:
    1. Normalize HS codes to 6-digit level
    2. Convert quantities to metric tons
    3. Convert values to USD
    4. Aggregate by (reporter, partner, flow, period, hs6)
    """

    def __init__(self):
        self.hs_normalizer = HSCodeNormalizer(target_level=6)
        self.unit_converter = UnitConverter()

    def harmonize_records(self, records: List[Dict]) -> List[HarmonizedRecord]:
        """
        Harmonize a list of trade records

        Args:
            records: Raw records from various sources

        Returns:
            List of HarmonizedRecord objects
        """
        # Convert to DataFrame for easier manipulation
        df = pd.DataFrame(records)

        if df.empty:
            return []

        # Normalize HS codes
        df['hs_code_6'] = df['hs_code'].apply(
            lambda x: self.hs_normalizer.normalize(x, None)
        )
        df['chapter'] = df['hs_code_6'].str[:2]

        # Convert quantities to tons
        if 'quantity_kg' in df.columns:
            df['quantity_tons'] = df['quantity_kg'].apply(
                lambda x: self.unit_converter.to_metric_tons(x, 'kg') if pd.notna(x) else None
            )
        elif 'quantity_tons' not in df.columns:
            df['quantity_tons'] = None

        # Ensure value_usd exists
        if 'value_usd' not in df.columns:
            df['value_usd'] = df.get('value_fob_usd', df.get('value_cif_usd', 0))

        # Aggregate by key dimensions
        agg_key = ['reporter_country', 'partner_country', 'flow', 'period',
                   'year', 'month', 'hs_code_6', 'chapter']

        # Only include columns that exist
        agg_key = [k for k in agg_key if k in df.columns]

        aggregated = df.groupby(agg_key, dropna=False).agg({
            'quantity_tons': 'sum',
            'value_usd': 'sum',
            'data_source': 'first',
            'hs_code': 'count',  # Count of source records
        }).reset_index()

        aggregated = aggregated.rename(columns={
            'data_source': 'source',
            'hs_code': 'source_count',
        })

        # Convert to HarmonizedRecord objects
        harmonized = []
        for _, row in aggregated.iterrows():
            try:
                record = HarmonizedRecord(
                    reporter_country=row.get('reporter_country', 'UNKNOWN'),
                    partner_country=row.get('partner_country', 'UNKNOWN'),
                    flow=row.get('flow', 'unknown'),
                    period=row.get('period', ''),
                    year=int(row.get('year', 0)),
                    month=int(row.get('month')) if pd.notna(row.get('month')) else None,
                    hs_code_6=row.get('hs_code_6', ''),
                    chapter=row.get('chapter', ''),
                    quantity_tons=row.get('quantity_tons'),
                    value_usd=float(row.get('value_usd', 0)),
                    source=row.get('source', 'UNKNOWN'),
                    source_count=int(row.get('source_count', 1)),
                )
                harmonized.append(record)
            except Exception as e:
                logger.warning(f"Error creating harmonized record: {e}")
                continue

        return harmonized

    def harmonize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Harmonize a DataFrame of trade records

        Args:
            df: DataFrame with raw trade records

        Returns:
            Harmonized DataFrame
        """
        records = df.to_dict('records')
        harmonized = self.harmonize_records(records)

        return pd.DataFrame([
            {
                'reporter_country': r.reporter_country,
                'partner_country': r.partner_country,
                'flow': r.flow,
                'period': r.period,
                'year': r.year,
                'month': r.month,
                'hs_code_6': r.hs_code_6,
                'chapter': r.chapter,
                'quantity_tons': r.quantity_tons,
                'value_usd': r.value_usd,
                'source': r.source,
                'source_count': r.source_count,
            }
            for r in harmonized
        ])


# =============================================================================
# TRADE BALANCE MATRIX
# =============================================================================

@dataclass
class BalanceEntry:
    """Entry in the trade balance matrix"""
    period: str
    hs_code_6: str
    exporter: str
    importer: str
    export_value_reported: Optional[float]
    import_value_reported: Optional[float]
    export_qty_reported: Optional[float]
    import_qty_reported: Optional[float]
    value_delta: Optional[float]
    value_delta_pct: Optional[float]
    balanced_value: Optional[float]
    balance_method: str


class BalanceMatrixBuilder:
    """
    Build reporter-partner-HS balance matrix for reconciliation

    Trade data from exporters and importers should theoretically match,
    but discrepancies occur due to:
    - Timing differences
    - Valuation (FOB vs CIF)
    - Re-exports
    - Misclassification
    - Reporting errors

    This class builds a matrix to identify and reconcile these discrepancies.
    """

    # Default balance methods
    BALANCE_METHODS = {
        'exporter_preferred': 'Use exporter-reported value',
        'importer_preferred': 'Use importer-reported value',
        'average': 'Simple average of both',
        'weighted_average': 'Weighted by reliability scores',
        'maximum': 'Use maximum reported value',
    }

    def __init__(self, default_method: str = 'exporter_preferred'):
        """
        Initialize balance matrix builder

        Args:
            default_method: Default balancing method
        """
        self.default_method = default_method

    def build_matrix(
        self,
        harmonized_data: List[HarmonizedRecord]
    ) -> List[BalanceEntry]:
        """
        Build balance matrix from harmonized data

        Args:
            harmonized_data: List of harmonized trade records

        Returns:
            List of BalanceEntry objects
        """
        # Convert to DataFrame
        df = pd.DataFrame([
            {
                'reporter': r.reporter_country,
                'partner': r.partner_country,
                'flow': r.flow,
                'period': r.period,
                'hs_code_6': r.hs_code_6,
                'value': r.value_usd,
                'quantity': r.quantity_tons,
            }
            for r in harmonized_data
        ])

        if df.empty:
            return []

        # Split into exports and imports
        exports = df[df['flow'] == 'export'].copy()
        imports = df[df['flow'] == 'import'].copy()

        # Rename for merging
        exports = exports.rename(columns={
            'value': 'export_value',
            'quantity': 'export_qty',
            'reporter': 'exporter',
            'partner': 'importer',
        })

        imports = imports.rename(columns={
            'value': 'import_value',
            'quantity': 'import_qty',
            'reporter': 'importer',
            'partner': 'exporter',
        })

        # Merge on common dimensions
        merged = pd.merge(
            exports[['period', 'hs_code_6', 'exporter', 'importer', 'export_value', 'export_qty']],
            imports[['period', 'hs_code_6', 'exporter', 'importer', 'import_value', 'import_qty']],
            on=['period', 'hs_code_6', 'exporter', 'importer'],
            how='outer',
        )

        # Calculate discrepancies
        balance_entries = []

        for _, row in merged.iterrows():
            export_val = row.get('export_value')
            import_val = row.get('import_value')

            # Calculate delta
            if pd.notna(export_val) and pd.notna(import_val):
                delta = export_val - import_val
                delta_pct = (delta / export_val * 100) if export_val != 0 else None
            else:
                delta = None
                delta_pct = None

            # Apply balancing
            balanced_value, method = self._apply_balance(
                export_val, import_val, self.default_method
            )

            entry = BalanceEntry(
                period=row['period'],
                hs_code_6=row['hs_code_6'],
                exporter=row['exporter'],
                importer=row['importer'],
                export_value_reported=export_val,
                import_value_reported=import_val,
                export_qty_reported=row.get('export_qty'),
                import_qty_reported=row.get('import_qty'),
                value_delta=delta,
                value_delta_pct=delta_pct,
                balanced_value=balanced_value,
                balance_method=method,
            )

            balance_entries.append(entry)

        return balance_entries

    def _apply_balance(
        self,
        export_val: Optional[float],
        import_val: Optional[float],
        method: str
    ) -> Tuple[Optional[float], str]:
        """
        Apply balancing method to derive single value

        Returns:
            Tuple of (balanced_value, method_used)
        """
        if pd.isna(export_val) and pd.isna(import_val):
            return None, 'no_data'

        if pd.isna(export_val):
            return import_val, 'importer_only'

        if pd.isna(import_val):
            return export_val, 'exporter_only'

        if method == 'exporter_preferred':
            return export_val, 'exporter_preferred'

        if method == 'importer_preferred':
            return import_val, 'importer_preferred'

        if method == 'average':
            return (export_val + import_val) / 2, 'average'

        if method == 'maximum':
            return max(export_val, import_val), 'maximum'

        # Default to exporter
        return export_val, 'exporter_preferred'

    def identify_discrepancies(
        self,
        balance_entries: List[BalanceEntry],
        threshold_pct: float = 10.0
    ) -> List[BalanceEntry]:
        """
        Identify significant discrepancies

        Args:
            balance_entries: List of balance entries
            threshold_pct: Percentage threshold for flagging

        Returns:
            List of entries with discrepancies above threshold
        """
        flagged = []

        for entry in balance_entries:
            if entry.value_delta_pct is not None:
                if abs(entry.value_delta_pct) > threshold_pct:
                    flagged.append(entry)

        return flagged

    def to_dataframe(self, balance_entries: List[BalanceEntry]) -> pd.DataFrame:
        """Convert balance entries to DataFrame"""
        return pd.DataFrame([
            {
                'period': e.period,
                'hs_code_6': e.hs_code_6,
                'exporter': e.exporter,
                'importer': e.importer,
                'export_value_reported': e.export_value_reported,
                'import_value_reported': e.import_value_reported,
                'export_qty_reported': e.export_qty_reported,
                'import_qty_reported': e.import_qty_reported,
                'value_delta': e.value_delta,
                'value_delta_pct': e.value_delta_pct,
                'balanced_value': e.balanced_value,
                'balance_method': e.balance_method,
            }
            for e in balance_entries
        ])

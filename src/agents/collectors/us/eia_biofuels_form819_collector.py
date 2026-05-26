"""
EIA Form 819 Monthly Biofuels Collector (xlsx)
==============================================

Downloads and ingests EIA Form 819 xlsx files into bronze:
  - bronze.eia_feedstock_monthly   (table2.xlsx — feedstock inputs by type)
  - bronze.eia_capacity_monthly    (table1.xlsx — operable capacity)

This complements the API-driven EIAMonthlyBiofuelsCollector which handles
finished-fuel series (production, stocks, imports, exports). Form 819
feedstock data is NOT exposed via the EIA v2 API — the xlsx is the only
public source.

Sources:
  https://www.eia.gov/biofuels/update/table1.xlsx
  https://www.eia.gov/biofuels/update/table2.xlsx

Each file is a rolling 2-year window; for deeper history use the
backfill script over archived xlsx files in data/raw/.

Released monthly, typically ~end-of-month for prior month (T-2 lag).
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

from .base_collector import (
    AuthType, BaseCollector, CollectorConfig, CollectorResult, DataFrequency,
)

logger = logging.getLogger(__name__)


PROJECT_ROOT = Path(__file__).resolve().parents[4]
DATA_DIR = PROJECT_ROOT / 'data' / 'eia_biofuels'

TABLE1_URL = "https://www.eia.gov/biofuels/update/table1.xlsx"
TABLE2_URL = "https://www.eia.gov/biofuels/update/table2.xlsx"


@dataclass
class EIABiofuelsForm819Config(CollectorConfig):
    source_name: str = "EIA Biofuels Form 819 (xlsx)"
    source_url: str = "https://www.eia.gov/biofuels/update/"
    auth_type: AuthType = AuthType.NONE
    frequency: DataFrequency = DataFrequency.MONTHLY
    table1_url: str = TABLE1_URL
    table2_url: str = TABLE2_URL
    download_dir: str = field(default_factory=lambda: str(DATA_DIR))


class EIABiofuelsForm819Collector(BaseCollector):
    """
    Dispatcher-compatible collector for EIA Form 819 xlsx downloads.

    Delegates download + parse + upsert to the existing functions in
    src.tools.eia_biofuels_collector so the same code paths serve both
    the CLI tool and the scheduled run.
    """

    def __init__(self, config: Optional[EIABiofuelsForm819Config] = None):
        config = config or EIABiofuelsForm819Config()
        super().__init__(config)
        self.config: EIABiofuelsForm819Config = config

    def get_table_name(self) -> str:
        return "eia_feedstock_monthly"

    def parse_response(self, response_data):
        return response_data

    def fetch_data(self, **kwargs) -> CollectorResult:
        raise NotImplementedError("Use collect() instead")

    def collect(self, **kwargs) -> CollectorResult:
        """
        Run the EIA Form 819 collector once.

        Steps:
          1. Download table1.xlsx (capacity) and table2.xlsx (feedstock)
          2. Parse each
          3. Upsert into bronze.eia_capacity_monthly + bronze.eia_feedstock_monthly
        """
        from src.tools.eia_biofuels_collector import (
            download_file, parse_table1, parse_table2,
            save_capacity_records, save_feedstock_records,
        )

        os.makedirs(self.config.download_dir, exist_ok=True)
        table1_path = os.path.join(self.config.download_dir, 'table1.xlsx')
        table2_path = os.path.join(self.config.download_dir, 'table2.xlsx')

        warnings = []
        cap_total = feed_total = 0

        # --- Table 1: Capacity ---
        try:
            download_file(self.config.table1_url, table1_path)
            cap_records = parse_table1(table1_path)
            self.logger.info(f"Table 1 parsed: {len(cap_records)} capacity records")
            if cap_records:
                ins, upd, err = save_capacity_records(cap_records, os.path.basename(table1_path))
                self.logger.info(f"Table 1 saved: {ins} inserted, {upd} updated, {err} errors")
                cap_total = ins + upd
                if err > 0:
                    warnings.append(f"Table 1: {err} save errors")
        except Exception as e:
            self.logger.error(f"Table 1 failed: {e}")
            warnings.append(f"Table 1: {e}")

        # --- Table 2: Feedstocks ---
        try:
            download_file(self.config.table2_url, table2_path)
            feed_records = parse_table2(table2_path)
            self.logger.info(f"Table 2 parsed: {len(feed_records)} feedstock records")
            if feed_records:
                ins, upd, err = save_feedstock_records(feed_records, os.path.basename(table2_path))
                self.logger.info(f"Table 2 saved: {ins} inserted, {upd} updated, {err} errors")
                feed_total = ins + upd
                if err > 0:
                    warnings.append(f"Table 2: {err} save errors")
        except Exception as e:
            self.logger.error(f"Table 2 failed: {e}")
            warnings.append(f"Table 2: {e}")

        total = cap_total + feed_total
        if total == 0:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message="No records ingested from either table",
                warnings=warnings,
            )

        return CollectorResult(
            success=True,
            source=self.config.source_name,
            records_fetched=total,
            data={'capacity': cap_total, 'feedstock': feed_total},
            period_start=datetime.now().isoformat(),
            warnings=warnings,
        )

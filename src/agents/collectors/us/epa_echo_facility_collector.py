"""
EPA ECHO Facility Collector — Dispatcher Wrapper

Thin dispatcher-compatible wrapper around the standalone EPA ECHO collector
in collectors/epa_echo/. Runs the collector, reads the resulting Excel output,
and upserts facility records into bronze.epa_echo_facility.

Registered as four separate schedule entries (one per profile):
    epa_echo_oilseed, epa_echo_ethanol, epa_echo_biodiesel, epa_echo_milling
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from .base_collector import (
    BaseCollector,
    CollectorConfig,
    CollectorResult,
    DataFrequency,
    AuthType,
)

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent


class EPAEchoFacilityCollector(BaseCollector):
    """
    Dispatcher-compatible collector that delegates to the standalone
    EPAEchoCollector and persists results to bronze.epa_echo_facility.

    Subclass per profile (see bottom of file) so the dispatcher registry
    can instantiate with no constructor args.
    """

    PROFILE: str = 'soybean_oilseed'  # overridden in subclasses

    def __init__(self):
        config = CollectorConfig(
            source_name=f'EPA ECHO ({self.PROFILE})',
            source_url='https://echodata.epa.gov/echo/',
            auth_type=AuthType.NONE,
            frequency=DataFrequency.MONTHLY,
        )
        super().__init__(config)

    def get_table_name(self) -> str:
        return 'epa_echo_facility'

    def fetch_data(self, **kwargs) -> CollectorResult:
        """Not used directly — collect() orchestrates the full workflow."""
        raise NotImplementedError("Use collect() instead")

    def parse_response(self, response_data):
        return response_data

    def collect(
        self,
        start_date=None,
        end_date=None,
        use_cache: bool = False,
        **kwargs,
    ) -> CollectorResult:
        """
        Run the EPA ECHO collector for this profile and save to bronze.

        Steps:
            1. Import and run the standalone EPAEchoCollector
            2. Read the facilities DataFrame it builds
            3. Upsert into bronze.epa_echo_facility
        """
        import sys
        sys.path.insert(0, str(PROJECT_ROOT))

        try:
            from collectors.epa_echo.epa_echo_collector import EPAEchoCollector
        except ImportError as e:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"Cannot import EPAEchoCollector: {e}",
            )

        self.logger.info(f"Running EPA ECHO collector (profile={self.PROFILE})")
        echo_collector = EPAEchoCollector(profile_name=self.PROFILE)
        result = echo_collector.collect()

        if not result.success:
            errors = result.errors if hasattr(result, 'errors') else []
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message='; '.join(errors) if errors else 'ECHO collection failed',
            )

        # Build the DataFrame from the collector's internal state
        facilities_df = echo_collector._build_facilities_df()
        total = len(facilities_df)
        self.logger.info(f"ECHO returned {total} facilities for {self.PROFILE}")

        if total == 0:
            return CollectorResult(
                success=True,
                source=self.config.source_name,
                records_fetched=0,
            )

        # Save to bronze
        saved = self.save_to_bronze(facilities_df)

        return CollectorResult(
            success=True,
            source=self.config.source_name,
            records_fetched=saved,
            period_start=datetime.now().isoformat(),
            period_end=datetime.now().isoformat(),
        )

    def save_to_bronze(self, df: pd.DataFrame) -> int:
        """Upsert facility records into bronze.epa_echo_facility."""
        from src.services.database.db_config import get_connection

        # Columns that map directly to the bronze table
        COLS = [
            'frs_registry_id', 'facility_name', 'street_address', 'city',
            'state', 'zip_code', 'county_name', 'county_fips', 'epa_region',
            'latitude', 'longitude', 'sic_codes', 'naics_codes',
            'dfr_naics', 'dfr_sic', 'operating_status', 'air_programs',
            'air_classification', 'source_id',
            'caa_permit_ids', 'npdes_permit_ids', 'rcra_handler_ids',
            'tri_facility_id', 'ghg_reporter_id',
            'compliance_status', 'enforcement_actions',
            'source_endpoint', 'collector_version', 'run_id',
        ]

        with get_connection() as conn:
            cur = conn.cursor()
            count = 0

            for _, row in df.iterrows():
                values = {}
                for col in COLS:
                    val = row.get(col, None)
                    # Convert pandas NaN/NaT to None
                    if pd.isna(val) if not isinstance(val, str) else False:
                        val = None
                    # Convert empty string to None for numeric fields
                    if col in ('latitude', 'longitude') and val == '':
                        val = None
                    values[col] = val

                values['search_profile'] = self.PROFILE
                values['collected_at'] = datetime.now()

                cur.execute("""
                    INSERT INTO bronze.epa_echo_facility
                        (frs_registry_id, facility_name, street_address, city,
                         state, zip_code, county_name, county_fips, epa_region,
                         latitude, longitude, sic_codes, naics_codes,
                         dfr_naics, dfr_sic, operating_status, air_programs,
                         air_classification, source_id,
                         caa_permit_ids, npdes_permit_ids, rcra_handler_ids,
                         tri_facility_id, ghg_reporter_id,
                         compliance_status, enforcement_actions,
                         search_profile, source_endpoint, collector_version,
                         collected_at)
                    VALUES
                        (%(frs_registry_id)s, %(facility_name)s, %(street_address)s,
                         %(city)s, %(state)s, %(zip_code)s, %(county_name)s,
                         %(county_fips)s, %(epa_region)s, %(latitude)s,
                         %(longitude)s, %(sic_codes)s, %(naics_codes)s,
                         %(dfr_naics)s, %(dfr_sic)s, %(operating_status)s,
                         %(air_programs)s, %(air_classification)s, %(source_id)s,
                         %(caa_permit_ids)s, %(npdes_permit_ids)s,
                         %(rcra_handler_ids)s, %(tri_facility_id)s,
                         %(ghg_reporter_id)s, %(compliance_status)s,
                         %(enforcement_actions)s, %(search_profile)s,
                         %(source_endpoint)s, %(collector_version)s,
                         %(collected_at)s)
                    ON CONFLICT (frs_registry_id)
                    DO UPDATE SET
                        facility_name = EXCLUDED.facility_name,
                        street_address = EXCLUDED.street_address,
                        city = EXCLUDED.city,
                        state = EXCLUDED.state,
                        zip_code = EXCLUDED.zip_code,
                        operating_status = EXCLUDED.operating_status,
                        sic_codes = EXCLUDED.sic_codes,
                        naics_codes = EXCLUDED.naics_codes,
                        dfr_naics = EXCLUDED.dfr_naics,
                        dfr_sic = EXCLUDED.dfr_sic,
                        compliance_status = EXCLUDED.compliance_status,
                        enforcement_actions = EXCLUDED.enforcement_actions,
                        caa_permit_ids = EXCLUDED.caa_permit_ids,
                        npdes_permit_ids = EXCLUDED.npdes_permit_ids,
                        rcra_handler_ids = EXCLUDED.rcra_handler_ids,
                        tri_facility_id = EXCLUDED.tri_facility_id,
                        ghg_reporter_id = EXCLUDED.ghg_reporter_id,
                        search_profile = EXCLUDED.search_profile,
                        collector_version = EXCLUDED.collector_version,
                        collected_at = EXCLUDED.collected_at,
                        updated_at = NOW()
                """, values)
                count += 1

            conn.commit()
            self.logger.info(
                f"Saved {count} facilities to bronze.epa_echo_facility "
                f"(profile={self.PROFILE})"
            )
            return count


# =============================================================================
# Profile-specific subclasses for dispatcher registration
# =============================================================================

class EPAEchoOilseedCollector(EPAEchoFacilityCollector):
    """Soybean & oilseed processing facilities (SIC 2075, 2076)."""
    PROFILE = 'soybean_oilseed'


class EPAEchoEthanolCollector(EPAEchoFacilityCollector):
    """Ethanol production facilities (SIC 2869)."""
    PROFILE = 'ethanol'


class EPAEchoBiodieselCollector(EPAEchoFacilityCollector):
    """Biodiesel & renewable diesel facilities (SIC 2911, 2992)."""
    PROFILE = 'biodiesel_renewable_diesel'


class EPAEchoMillingCollector(EPAEchoFacilityCollector):
    """Wheat & flour milling facilities (SIC 2041-2046)."""
    PROFILE = 'wheat_milling'

"""
Census Bronze Collector Agent
=============================
Agent for collecting Census Bureau trade data and saving to bronze layer.
Uses the HS codes reference file for comprehensive commodity coverage.

Logs all activities for the CensusLogReaderAgent daily summary.
"""

import json
import os
import sys
import time
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / 'src'))

from agents.base.census_base_agent import CensusBaseAgent, PipelineLayer, EventType
from agents.collectors.us.census_trade_collector_v2 import (
    CensusTradeCollectorV2,
    CensusTradeConfig,
    CENSUS_SCHEDULE_B_CODES,
    load_hs_codes_reference,
    get_hs_codes_for_category
)


class CensusBronzeCollectorAgent(CensusBaseAgent):
    """
    Agent for collecting Census Bureau trade data.

    Fetches monthly trade data for all configured HS codes and saves
    to the bronze.census_trade table.
    """

    def __init__(self, **kwargs):
        super().__init__(agent_name='CensusBronzeCollector', **kwargs)

        # Initialize the collector
        self.collector = CensusTradeCollectorV2()

        # Load HS codes reference
        self.hs_reference = load_hs_codes_reference()

        self.log_event(
            EventType.INFO,
            f"Initialized with {len(CENSUS_SCHEDULE_B_CODES)} commodity codes"
        )

    def get_layer(self) -> PipelineLayer:
        return PipelineLayer.BRONZE

    def run(
        self,
        start_date: date = None,
        end_date: date = None,
        commodity_groups: List[str] = None,
        flow: str = 'both'
    ):
        """
        Collect Census trade data and save to bronze layer.

        Args:
            start_date: Start date (default: 13 months ago)
            end_date: End date (default: previous month)
            commodity_groups: List of commodity groups to collect (default: all)
            flow: 'imports', 'exports', or 'both'

        Returns:
            AgentResult with collection statistics
        """
        # Set default date range
        if end_date is None:
            # Default to previous complete month
            today = date.today()
            if today.month == 1:
                end_date = date(today.year - 1, 12, 1)
            else:
                end_date = date(today.year, today.month - 1, 1)

        if start_date is None:
            # Default to 13 months before end date
            if end_date.month <= 1:
                start_date = date(end_date.year - 2, end_date.month + 11, 1)
            else:
                start_date = date(end_date.year - 1, end_date.month - 1, 1)

        # Ensure minimum date of 2013
        if start_date < date(2013, 1, 1):
            start_date = date(2013, 1, 1)

        self.set_metadata('start_date', start_date.isoformat())
        self.set_metadata('end_date', end_date.isoformat())
        self.set_metadata('flow', flow)

        self.log_event(
            EventType.DATA_FETCH,
            f"Collecting Census trade data from {start_date} to {end_date}",
            data={'start_date': str(start_date), 'end_date': str(end_date), 'flow': flow}
        )

        # Determine which HS codes to collect
        hs_codes_to_collect = self._get_hs_codes(commodity_groups)
        self.set_metadata('hs_codes_count', len(hs_codes_to_collect))

        self.log_event(
            EventType.INFO,
            f"Collecting {len(hs_codes_to_collect)} HS codes",
            data={'hs_codes': list(hs_codes_to_collect.keys())[:10]}  # Log first 10
        )

        # Collect data
        flows = ['imports', 'exports'] if flow == 'both' else [flow]
        all_records = []
        collection_stats = {
            'hs_codes_attempted': 0,
            'hs_codes_with_data': 0,
            'api_errors': 0
        }

        start_time = time.time()

        for trade_flow in flows:
            for commodity_name, hs_code in hs_codes_to_collect.items():
                collection_stats['hs_codes_attempted'] += 1

                try:
                    records = self.collector.fetch_trade_data(
                        flow=trade_flow,
                        hs_code=hs_code,
                        start_date=start_date,
                        end_date=end_date
                    )

                    if records:
                        all_records.extend(records)
                        collection_stats['hs_codes_with_data'] += 1
                        self.log_data_event(
                            action='fetch',
                            records_count=len(records),
                            commodity=commodity_name,
                            hs_code=hs_code,
                            flow=trade_flow
                        )

                except Exception as e:
                    collection_stats['api_errors'] += 1
                    self.log_warning(
                        f"Error fetching {trade_flow}/{commodity_name}: {e}",
                        data={'hs_code': hs_code, 'flow': trade_flow}
                    )

        fetch_duration_ms = int((time.time() - start_time) * 1000)
        self.add_records_processed(len(all_records))

        self.log_event(
            EventType.DATA_FETCH,
            f"Fetched {len(all_records)} total records",
            data=collection_stats,
            duration_ms=fetch_duration_ms
        )

        # Save to bronze layer
        if all_records:
            save_start = time.time()

            try:
                db_stats = self.collector.save_to_bronze(all_records, self.conn)

                self.add_records_inserted(db_stats.get('inserted', 0))
                self.add_records_updated(db_stats.get('updated', 0))
                self.add_records_failed(db_stats.get('errors', 0))

                save_duration_ms = int((time.time() - save_start) * 1000)

                self.log_data_event(
                    action='save',
                    records_count=len(all_records),
                    duration_ms=save_duration_ms
                )

                self.log_event(
                    EventType.DATA_SAVE,
                    f"Saved to bronze.census_trade",
                    data=db_stats,
                    duration_ms=save_duration_ms
                )

            except Exception as e:
                self.log_error(
                    f"Failed to save to bronze layer: {e}",
                    data={'records_count': len(all_records)}
                )

        else:
            self.log_warning("No records collected")

        self.set_metadata('collection_stats', collection_stats)

        return self.complete()

    def _get_hs_codes(self, commodity_groups: List[str] = None) -> Dict[str, str]:
        """
        Get HS codes to collect.

        Args:
            commodity_groups: List of groups to collect (or None for all)

        Returns:
            Dict mapping commodity name to HS code
        """
        # If specific groups requested, get codes for those groups
        if commodity_groups:
            codes = {}
            category_map = {
                'grains': 'grains',
                'oilseeds': 'oilseeds',
                'oils': 'vegetable_oils',
                'meals': 'protein_meals',
                'biofuels': 'biofuels',
                'fats': 'animal_fats',
                'livestock': 'livestock_meat',
                'petroleum': 'petroleum'
            }

            for group in commodity_groups:
                category = category_map.get(group, group)
                group_codes = get_hs_codes_for_category(category)
                for code in group_codes:
                    codes[code] = code

            return codes

        # Otherwise use all known Census Schedule B codes
        # Filter out 4-digit summary codes
        return {
            name: code for name, code in CENSUS_SCHEDULE_B_CODES.items()
            if len(code) >= 8
        }

    def collect_commodity(
        self,
        commodity: str,
        start_date: date = None,
        end_date: date = None,
        flow: str = 'both'
    ):
        """
        Collect data for a specific commodity.

        Args:
            commodity: Commodity key from CENSUS_SCHEDULE_B_CODES
            start_date: Start date
            end_date: End date
            flow: 'imports', 'exports', or 'both'
        """
        hs_code = CENSUS_SCHEDULE_B_CODES.get(commodity)
        if not hs_code:
            self.log_error(f"Unknown commodity: {commodity}")
            return self.complete(success=False)

        # Set date range
        end_date = end_date or date.today().replace(day=1) - timedelta(days=1)
        start_date = start_date or date(end_date.year - 1, end_date.month, 1)

        self.log_event(
            EventType.DATA_FETCH,
            f"Collecting {commodity} ({hs_code})",
            data={'commodity': commodity, 'hs_code': hs_code}
        )

        flows = ['imports', 'exports'] if flow == 'both' else [flow]
        all_records = []

        for trade_flow in flows:
            try:
                records = self.collector.fetch_trade_data(
                    flow=trade_flow,
                    hs_code=hs_code,
                    start_date=start_date,
                    end_date=end_date
                )
                all_records.extend(records)
                self.log_data_event(
                    action='fetch',
                    records_count=len(records),
                    commodity=commodity,
                    hs_code=hs_code,
                    flow=trade_flow
                )
            except Exception as e:
                self.log_error(f"Error fetching {trade_flow}/{commodity}: {e}")

        self.add_records_processed(len(all_records))

        if all_records:
            db_stats = self.collector.save_to_bronze(all_records, self.conn)
            self.add_records_inserted(db_stats.get('inserted', 0))
            self.add_records_updated(db_stats.get('updated', 0))
            self.log_data_event(action='save', records_count=len(all_records))

        return self.complete()


# =============================================================================
# CLI
# =============================================================================

def main():
    """CLI for Census Bronze Collector Agent"""
    import argparse
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    parser = argparse.ArgumentParser(description='Census Bronze Collector Agent')

    parser.add_argument(
        '--start-date',
        help='Start date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end-date',
        help='End date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--commodity',
        help='Specific commodity to collect'
    )
    parser.add_argument(
        '--groups',
        nargs='+',
        help='Commodity groups to collect'
    )
    parser.add_argument(
        '--flow',
        choices=['imports', 'exports', 'both'],
        default='both',
        help='Trade flow to collect'
    )

    args = parser.parse_args()

    # Parse dates
    start_date = None
    end_date = None
    if args.start_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
    if args.end_date:
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()

    # Run agent
    agent = CensusBronzeCollectorAgent()

    if args.commodity:
        result = agent.collect_commodity(
            commodity=args.commodity,
            start_date=start_date,
            end_date=end_date,
            flow=args.flow
        )
    else:
        result = agent.run(
            start_date=start_date,
            end_date=end_date,
            commodity_groups=args.groups,
            flow=args.flow
        )

    print(f"\nResult: {'SUCCESS' if result.success else 'FAILED'}")
    print(f"Records processed: {result.records_processed}")
    print(f"Records inserted: {result.records_inserted}")
    print(f"Records updated: {result.records_updated}")
    print(f"Duration: {result.duration_seconds:.1f}s")

    if result.errors:
        print(f"\nErrors: {len(result.errors)}")
        for error in result.errors[:5]:
            print(f"  - {error}")


if __name__ == '__main__':
    main()

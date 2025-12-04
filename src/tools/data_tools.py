"""
Data Collection Tools - Tools for fetching data from external sources.

Wraps existing collector functionality as modular tools.
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from .registry import Tool, ToolResult

logger = logging.getLogger(__name__)


class USDAFetchTool(Tool):
    """Tool for fetching USDA AMS data"""

    @property
    def name(self) -> str:
        return "fetch_usda_data"

    @property
    def description(self) -> str:
        return "Fetch commodity price data from USDA AMS API"

    @property
    def parameters(self) -> Dict[str, Any]:
        return {
            'report_date': {
                'type': 'string',
                'required': False,
                'description': 'Date to fetch (MM/DD/YYYY format), defaults to today'
            },
            'report_ids': {
                'type': 'array',
                'required': False,
                'description': 'List of specific report IDs to fetch'
            },
            'commodity_filter': {
                'type': 'string',
                'required': False,
                'description': 'Filter results by commodity name'
            },
            'location_filter': {
                'type': 'string',
                'required': False,
                'description': 'Filter results by location'
            }
        }

    def _get_collector(self):
        """Get or create a USDA collector instance"""
        try:
            # Try the api Manager location first
            api_manager_path = Path(__file__).parent.parent.parent / 'api Manager'
            if str(api_manager_path) not in sys.path:
                sys.path.insert(0, str(api_manager_path))

            from usda_ams_collector_asynch import USDACollector
            return USDACollector()
        except ImportError:
            try:
                # Try the commodity pipeline location
                pipeline_path = Path(__file__).parent.parent.parent / 'commodity_pipeline' / 'usda_ams_agent'
                if str(pipeline_path) not in sys.path:
                    sys.path.insert(0, str(pipeline_path))

                from usda_ams_collector_asynch import USDACollector
                return USDACollector()
            except ImportError as e:
                logger.warning(f"Could not import USDACollector: {e}")
                return None

    async def execute(self, params: Dict[str, Any]) -> ToolResult:
        """Execute the USDA data fetch"""
        try:
            collector = self._get_collector()
            if collector is None:
                return ToolResult(
                    success=False,
                    error="USDACollector not available"
                )

            report_date = params.get('report_date')

            # Collect data
            records = await collector.collect_daily_prices(report_date=report_date)

            # Apply filters if specified
            commodity_filter = params.get('commodity_filter')
            location_filter = params.get('location_filter')

            if commodity_filter or location_filter:
                filtered_records = []
                for record in records:
                    commodity_match = True
                    location_match = True

                    if commodity_filter:
                        commodity = str(record.get('commodity', '')).lower()
                        commodity_match = commodity_filter.lower() in commodity

                    if location_filter:
                        location = str(record.get('location', '')).lower()
                        location_match = location_filter.lower() in location

                    if commodity_match and location_match:
                        filtered_records.append(record)

                records = filtered_records

            return ToolResult(
                success=True,
                data={
                    'records': records,
                    'count': len(records),
                    'report_date': report_date or datetime.now().strftime('%m/%d/%Y')
                },
                metadata={
                    'filters_applied': {
                        'commodity': commodity_filter,
                        'location': location_filter
                    }
                }
            )

        except Exception as e:
            logger.error(f"USDA data fetch failed: {e}")
            return ToolResult(
                success=False,
                error=str(e)
            )


class USDAHistoricalTool(Tool):
    """Tool for fetching historical USDA data"""

    @property
    def name(self) -> str:
        return "fetch_usda_historical"

    @property
    def description(self) -> str:
        return "Fetch historical commodity price data from USDA AMS API"

    @property
    def parameters(self) -> Dict[str, Any]:
        return {
            'start_date': {
                'type': 'string',
                'required': True,
                'description': 'Start date (MM/DD/YYYY format)'
            },
            'end_date': {
                'type': 'string',
                'required': False,
                'description': 'End date (MM/DD/YYYY format), defaults to today'
            },
            'report_ids': {
                'type': 'array',
                'required': False,
                'description': 'List of specific report IDs to fetch'
            },
            'commodity_filter': {
                'type': 'string',
                'required': False,
                'description': 'Filter results by commodity name'
            }
        }

    async def validate_params(self, params: Dict[str, Any]) -> Optional[str]:
        if 'start_date' not in params:
            return "Parameter 'start_date' is required"
        return None

    def _get_collector(self):
        """Get or create a USDA collector instance"""
        try:
            api_manager_path = Path(__file__).parent.parent.parent / 'api Manager'
            if str(api_manager_path) not in sys.path:
                sys.path.insert(0, str(api_manager_path))

            from usda_ams_collector_asynch import USDACollector
            return USDACollector()
        except ImportError as e:
            logger.warning(f"Could not import USDACollector: {e}")
            return None

    async def execute(self, params: Dict[str, Any]) -> ToolResult:
        """Execute historical data fetch"""
        try:
            collector = self._get_collector()
            if collector is None:
                return ToolResult(
                    success=False,
                    error="USDACollector not available"
                )

            start_date = params['start_date']
            end_date = params.get('end_date')
            report_ids = params.get('report_ids')
            commodity_filter = params.get('commodity_filter')

            # Collect historical data
            records = await collector.collect_historical_data(
                start_date=start_date,
                end_date=end_date,
                report_ids=report_ids,
                commodity_filter=commodity_filter
            )

            return ToolResult(
                success=True,
                data={
                    'records': records,
                    'count': len(records),
                    'date_range': {
                        'start': start_date,
                        'end': end_date or datetime.now().strftime('%m/%d/%Y')
                    }
                }
            )

        except Exception as e:
            logger.error(f"USDA historical fetch failed: {e}")
            return ToolResult(
                success=False,
                error=str(e)
            )


class DataValidationTool(Tool):
    """Tool for validating data quality"""

    @property
    def name(self) -> str:
        return "validate_data"

    @property
    def description(self) -> str:
        return "Validate data records for quality and completeness"

    @property
    def parameters(self) -> Dict[str, Any]:
        return {
            'records': {
                'type': 'array',
                'required': True,
                'description': 'Records to validate'
            },
            'required_fields': {
                'type': 'array',
                'required': False,
                'description': 'List of field names that must be present'
            },
            'check_prices': {
                'type': 'boolean',
                'required': False,
                'default': True,
                'description': 'Whether to validate price values'
            }
        }

    async def validate_params(self, params: Dict[str, Any]) -> Optional[str]:
        if 'records' not in params:
            return "Parameter 'records' is required"
        return None

    async def execute(self, params: Dict[str, Any]) -> ToolResult:
        """Execute data validation"""
        try:
            records = params['records']
            required_fields = params.get('required_fields', ['commodity', 'report_date'])
            check_prices = params.get('check_prices', True)

            validation_results = {
                'total_records': len(records),
                'valid_records': 0,
                'invalid_records': 0,
                'issues': []
            }

            for i, record in enumerate(records):
                issues = []

                # Check required fields
                for field in required_fields:
                    if field not in record or record[field] is None or record[field] == '':
                        issues.append(f"Missing required field: {field}")

                # Check price validity
                if check_prices:
                    price_fields = ['price', 'price_avg', 'price_low', 'price_high']
                    has_price = any(
                        field in record and record[field] is not None
                        for field in price_fields
                    )
                    if not has_price:
                        issues.append("No price data found")

                    # Check for negative prices
                    for field in price_fields:
                        if field in record and record[field] is not None:
                            try:
                                if float(record[field]) < 0:
                                    issues.append(f"Negative price in {field}")
                            except (ValueError, TypeError):
                                issues.append(f"Invalid price value in {field}")

                if issues:
                    validation_results['invalid_records'] += 1
                    if len(validation_results['issues']) < 10:  # Limit reported issues
                        validation_results['issues'].append({
                            'record_index': i,
                            'issues': issues
                        })
                else:
                    validation_results['valid_records'] += 1

            validation_results['validity_rate'] = (
                validation_results['valid_records'] / max(len(records), 1)
            )

            return ToolResult(
                success=validation_results['validity_rate'] > 0.8,
                data=validation_results
            )

        except Exception as e:
            logger.error(f"Data validation failed: {e}")
            return ToolResult(
                success=False,
                error=str(e)
            )

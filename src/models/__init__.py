"""
Shared data models for the RLC Agent System.

Contains dataclasses and types used across the system.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class PriceRecord:
    """A standardized commodity price record"""
    commodity: str
    report_date: str
    source: str
    source_report: str

    # Price fields
    price: Optional[float] = None
    price_low: Optional[float] = None
    price_high: Optional[float] = None
    price_avg: Optional[float] = None

    # Basis fields
    basis: Optional[float] = None
    basis_low: Optional[float] = None
    basis_high: Optional[float] = None

    # Location and metadata
    location: Optional[str] = None
    unit: Optional[str] = None
    grade: Optional[str] = None
    delivery_period: Optional[str] = None
    report_type: Optional[str] = None

    # Timestamps
    fetch_timestamp: Optional[str] = None
    created_at: Optional[str] = None

    # Raw data for debugging
    raw_data: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'commodity': self.commodity,
            'report_date': self.report_date,
            'source': self.source,
            'source_report': self.source_report,
            'price': self.price,
            'price_low': self.price_low,
            'price_high': self.price_high,
            'price_avg': self.price_avg,
            'basis': self.basis,
            'basis_low': self.basis_low,
            'basis_high': self.basis_high,
            'location': self.location,
            'unit': self.unit,
            'grade': self.grade,
            'delivery_period': self.delivery_period,
            'report_type': self.report_type,
            'fetch_timestamp': self.fetch_timestamp
        }


@dataclass
class DataSourceInfo:
    """Information about a data source"""
    source_id: str
    name: str
    source_type: str  # usda, trade, export, etc.
    frequency: str  # daily, weekly, monthly
    enabled: bool = True
    last_collection: Optional[datetime] = None
    record_count: int = 0


@dataclass
class SystemStatus:
    """Overall system status"""
    healthy: bool
    timestamp: datetime
    components: Dict[str, bool] = field(default_factory=dict)
    active_tasks: int = 0
    pending_tasks: int = 0
    database_records: int = 0
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'healthy': self.healthy,
            'timestamp': self.timestamp.isoformat(),
            'components': self.components,
            'active_tasks': self.active_tasks,
            'pending_tasks': self.pending_tasks,
            'database_records': self.database_records,
            'errors': self.errors
        }

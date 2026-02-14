"""
EPA ECHO Facility Data Collector

Collects facility-level environmental compliance data from the EPA's
Enforcement and Compliance History Online (ECHO) database.

Facility types supported:
- Soybean and oilseed processing plants (SIC 2075, 2076)
- Future: ethanol, biodiesel, renewable diesel, wheat mills
"""

from .epa_echo_collector import EPAEchoCollector
from .epa_echo_checker import EPAEchoChecker

__all__ = ['EPAEchoCollector', 'EPAEchoChecker']

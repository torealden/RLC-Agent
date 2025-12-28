"""
FGIS CSV Parser
Parses the Federal Grain Inspection Service export inspection CSV files
Handles the 112-column format with proper data type conversion
"""

import csv
import logging
from dataclasses import dataclass, field
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, List, Optional, Generator, Any, Tuple
import re

logger = logging.getLogger(__name__)


@dataclass
class CSVColumnMapping:
    """
    Maps CSV column names to database field names
    Based on the FGIS CSV header structure
    """
    # Column name mapping: CSV Header -> Database Field
    column_map: Dict[str, str] = field(default_factory=lambda: {
        # Temporal
        'Thursday': 'week_ending_date',
        'Cert Date': 'cert_date',
        'MKT YR': 'marketing_year',
        
        # Identifiers
        'Serial No.': 'serial_number',
        'Type Shipm': 'type_shipment',
        'Type Serv': 'type_service',
        'Type Carrier': 'type_carrier',
        'Carrier Name': 'carrier_name',
        
        # Location
        'Field Office': 'field_office',
        'Port': 'port',
        'AMS Reg': 'ams_region',
        'FGIS Reg': 'fgis_region',
        'City': 'city',
        'State': 'state',
        
        # Commodity
        'Grain': 'grain',
        'Class': 'commodity_class',
        'SubClass': 'subclass',
        'Grade': 'grade',
        'Spec Gr 1': 'special_grade_1',
        'Spec Gr 2': 'special_grade_2',
        
        # Destination
        'Destination': 'destination',
        
        # Quantity
        'Pounds': 'pounds',
        'Metric Ton': 'metric_tons',
        '1000 Bushels': 'thousand_bushels',  # Pre-2014 only
        'Subl/Carrs': 'sublot_carriers',
        
        # Dockage
        'DKG HIGH': 'dockage_high',
        'DKG LOW': 'dockage_low',
        'DKG AVG': 'dockage_avg',
        
        # Test Weight
        'TW': 'test_weight',
        
        # Moisture
        'M HIGH': 'moisture_high',
        'M LOW': 'moisture_low',
        'M AVG': 'moisture_avg',
        
        # Broken Corn & FM
        'BCFM HIGH': 'broken_corn_fm_high',
        'BCFM LOW': 'broken_corn_fm_low',
        'BCFM AVG': 'broken_corn_fm_avg',
        
        # Total Damage
        'DM HIGH': 'total_damage_high',
        'DM LOW': 'total_damage_low',
        'DM AVG': 'total_damage_avg',
        
        # Heat Damage
        'HD HIGH': 'heat_damage_high',
        'HD LOW': 'heat_damage_low',
        'HD AVG': 'heat_damage_avg',
        
        # Foreign Material
        'FM HIGH': 'foreign_material_high',
        'FM LOW': 'foreign_material_low',
        'FM AVG': 'foreign_material_avg',
        
        # Shrunken & Broken
        'SB HIGH': 'shrunken_broken_high',
        'SB LOW': 'shrunken_broken_low',
        'SB AVG': 'shrunken_broken_avg',
        
        # Total Defects
        'DEF HIGH': 'total_defects_high',
        'DEF LOW': 'total_defects_low',
        'DEF AVG': 'total_defects_avg',
        
        # Splits
        'SPL HIGH': 'splits_high',
        'SPL LOW': 'splits_low',
        'SPL AVG': 'splits_avg',
        
        # Protein
        'PRO HIGH': 'protein_high',
        'PRO LOW': 'protein_low',
        'PRO AVG': 'protein_avg',
        'PROT BASIS': 'protein_basis',
        
        # Oil
        'OIL HIGH': 'oil_high',
        'OIL LOW': 'oil_low',
        'OIL AVG': 'oil_avg',
        'OIL BASIS': 'oil_basis',
        
        # Starch
        'STARCH HIGH': 'starch_high',
        'STARCH LOW': 'starch_low',
        'STARCH AVG': 'starch_avg',
        
        # Aflatoxin
        'AFLA REQ': 'aflatoxin_required',
        'AFLA BASIS': 'aflatoxin_basis',
        'AFLA AVG PPB': 'aflatoxin_avg_ppb',
        'AFLA REJ': 'aflatoxin_rejected',
        
        # DON (Vomitoxin)
        'DON REQ': 'don_required',
        'DON BASIS': 'don_basis',
        'DON AVG PPM': 'don_avg_ppm',
        'DON REJ': 'don_rejected',
        
        # Pest
        'Fumigant': 'fumigant',
        'Insecticide': 'insecticide',
        'Insect #': 'insect_count',
        
        # Additional
        'FN': 'falling_number',
        'Odor': 'odor',
    })
    
    # Date columns that need parsing
    date_columns: List[str] = field(default_factory=lambda: [
        'week_ending_date', 'cert_date'
    ])
    
    # Integer columns
    integer_columns: List[str] = field(default_factory=lambda: [
        'pounds', 'marketing_year', 'sublot_carriers', 'insect_count'
    ])
    
    # Decimal columns (all quality metrics)
    decimal_columns: List[str] = field(default_factory=lambda: [
        'metric_tons', 'thousand_bushels',
        'dockage_high', 'dockage_low', 'dockage_avg',
        'test_weight',
        'moisture_high', 'moisture_low', 'moisture_avg',
        'broken_corn_fm_high', 'broken_corn_fm_low', 'broken_corn_fm_avg',
        'total_damage_high', 'total_damage_low', 'total_damage_avg',
        'heat_damage_high', 'heat_damage_low', 'heat_damage_avg',
        'foreign_material_high', 'foreign_material_low', 'foreign_material_avg',
        'shrunken_broken_high', 'shrunken_broken_low', 'shrunken_broken_avg',
        'total_defects_high', 'total_defects_low', 'total_defects_avg',
        'splits_high', 'splits_low', 'splits_avg',
        'protein_high', 'protein_low', 'protein_avg',
        'oil_high', 'oil_low', 'oil_avg',
        'starch_high', 'starch_low', 'starch_avg',
        'aflatoxin_avg_ppb', 'don_avg_ppm', 'falling_number'
    ])
    
    # Boolean columns
    boolean_columns: List[str] = field(default_factory=lambda: [
        'aflatoxin_required', 'aflatoxin_rejected',
        'don_required', 'don_rejected',
        'musty', 'sour'
    ])


@dataclass
class ParsedRecord:
    """Represents a single parsed inspection record"""
    data: Dict[str, Any]
    row_number: int
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    @property
    def is_valid(self) -> bool:
        return len(self.errors) == 0
    
    @property
    def has_warnings(self) -> bool:
        return len(self.warnings) > 0


class FGISCSVParser:
    """
    Parser for FGIS Export Grain Inspection CSV files
    """
    
    def __init__(self, mapping: Optional[CSVColumnMapping] = None):
        self.mapping = mapping or CSVColumnMapping()
        self.stats = {
            'rows_read': 0,
            'rows_parsed': 0,
            'rows_with_errors': 0,
            'rows_with_warnings': 0,
        }
    
    def parse_date(self, value: str, formats: List[str] = None) -> Optional[date]:
        """Parse date from various formats"""
        if not value or value.strip() == '':
            return None
        
        formats = formats or [
            '%m/%d/%Y',      # MM/DD/YYYY
            '%Y-%m-%d',      # YYYY-MM-DD
            '%m-%d-%Y',      # MM-DD-YYYY
            '%d-%b-%Y',      # DD-Mon-YYYY
            '%m/%d/%y',      # MM/DD/YY
            '%Y%m%d',        # YYYYMMDD (no separators)
        ]
        
        value = value.strip()
        for fmt in formats:
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                continue
        
        return None
    
    def parse_integer(self, value: str) -> Optional[int]:
        """Parse integer, handling commas and empty strings"""
        if not value or value.strip() == '':
            return None
        
        try:
            # Remove commas and whitespace
            clean_value = value.replace(',', '').strip()
            return int(float(clean_value))  # Handle "1234.0" format
        except (ValueError, TypeError):
            return None
    
    def parse_decimal(self, value: str, precision: int = 4) -> Optional[Decimal]:
        """Parse decimal number"""
        if not value or value.strip() == '':
            return None
        
        try:
            clean_value = value.replace(',', '').strip()
            return round(Decimal(clean_value), precision)
        except (InvalidOperation, ValueError, TypeError):
            return None
    
    def parse_boolean(self, value: str) -> Optional[bool]:
        """Parse boolean from various representations"""
        if not value or value.strip() == '':
            return None
        
        value = value.strip().upper()
        
        if value in ('Y', 'YES', 'TRUE', '1', 'T'):
            return True
        elif value in ('N', 'NO', 'FALSE', '0', 'F'):
            return False
        
        return None
    
    def clean_string(self, value: str, max_length: int = None) -> Optional[str]:
        """Clean and optionally truncate string value"""
        if not value:
            return None
        
        cleaned = value.strip()
        if not cleaned:
            return None
        
        if max_length and len(cleaned) > max_length:
            cleaned = cleaned[:max_length]
        
        return cleaned
    
    def parse_row(self, row: Dict[str, str], row_number: int, 
                  source_file: str = None, calendar_year: int = None) -> ParsedRecord:
        """
        Parse a single CSV row into a structured record
        """
        data = {}
        errors = []
        warnings = []
        
        # Build reverse mapping (CSV header -> db field)
        header_to_field = self.mapping.column_map
        
        # Process each column
        for csv_header, value in row.items():
            # Skip empty headers
            if not csv_header or not csv_header.strip():
                continue
            
            csv_header = csv_header.strip()
            
            # Get the database field name
            db_field = header_to_field.get(csv_header)
            if not db_field:
                # Try case-insensitive match
                for h, f in header_to_field.items():
                    if h.upper() == csv_header.upper():
                        db_field = f
                        break
            
            if not db_field:
                # Unknown column - skip silently (many columns not needed)
                continue
            
            # Parse based on field type
            try:
                if db_field in self.mapping.date_columns:
                    parsed_value = self.parse_date(value)
                    if value and value.strip() and parsed_value is None:
                        warnings.append(f"Could not parse date '{value}' for {db_field}")
                elif db_field in self.mapping.integer_columns:
                    parsed_value = self.parse_integer(value)
                elif db_field in self.mapping.decimal_columns:
                    parsed_value = self.parse_decimal(value)
                elif db_field in self.mapping.boolean_columns:
                    parsed_value = self.parse_boolean(value)
                else:
                    parsed_value = self.clean_string(value)
                
                data[db_field] = parsed_value
                
            except Exception as e:
                warnings.append(f"Error parsing {db_field}: {str(e)}")
                data[db_field] = None
        
        # Validate required fields
        required_fields = ['week_ending_date', 'serial_number', 'grain', 'destination', 'pounds']
        for field in required_fields:
            if field not in data or data[field] is None:
                errors.append(f"Missing required field: {field}")
        
        # Add metadata
        if source_file:
            data['source_file'] = source_file
        
        if calendar_year:
            data['calendar_year'] = calendar_year
        elif data.get('week_ending_date'):
            data['calendar_year'] = data['week_ending_date'].year
        
        # Calculate bushels from pounds if not present
        if data.get('pounds') and 'bushels' not in data:
            grain = data.get('grain', '').upper()
            bushel_weight = self._get_bushel_weight(grain)
            data['bushels'] = Decimal(str(data['pounds'])) / Decimal(str(bushel_weight))
        
        return ParsedRecord(
            data=data,
            row_number=row_number,
            errors=errors,
            warnings=warnings
        )
    
    def _get_bushel_weight(self, grain: str) -> float:
        """Get bushel weight for grain type"""
        weights = {
            'SOYBEANS': 60.0,
            'CORN': 56.0,
            'WHEAT': 60.0,
            'SORGHUM': 56.0,
            'BARLEY': 48.0,
            'OATS': 32.0,
            'RYE': 56.0,
            'FLAXSEED': 56.0,
            'SUNFLOWER': 28.0,
        }
        return weights.get(grain.upper(), 60.0)
    
    def parse_file(self, file_path: Path, 
                   calendar_year: int = None) -> Generator[ParsedRecord, None, None]:
        """
        Parse entire CSV file, yielding records one at a time
        
        Args:
            file_path: Path to CSV file
            calendar_year: Override calendar year (useful for historical files)
            
        Yields:
            ParsedRecord for each row
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")
        
        # Extract year from filename if not provided (e.g., CY2025.csv)
        if calendar_year is None:
            year_match = re.search(r'(?:CY)?(\d{4})', file_path.stem)
            if year_match:
                calendar_year = int(year_match.group(1))
        
        source_file = file_path.name
        
        # Reset stats
        self.stats = {
            'rows_read': 0,
            'rows_parsed': 0,
            'rows_with_errors': 0,
            'rows_with_warnings': 0,
        }
        
        logger.info(f"Starting to parse {file_path}")
        
        # Try different encodings
        encodings = ['utf-8', 'latin-1', 'cp1252']
        
        for encoding in encodings:
            try:
                with open(file_path, 'r', encoding=encoding, newline='') as f:
                    reader = csv.DictReader(f)
                    
                    for row_num, row in enumerate(reader, start=2):  # Start at 2 (header is row 1)
                        self.stats['rows_read'] += 1
                        
                        record = self.parse_row(
                            row=row,
                            row_number=row_num,
                            source_file=source_file,
                            calendar_year=calendar_year
                        )
                        
                        if record.is_valid:
                            self.stats['rows_parsed'] += 1
                        else:
                            self.stats['rows_with_errors'] += 1
                            logger.warning(f"Row {row_num} errors: {record.errors}")
                        
                        if record.has_warnings:
                            self.stats['rows_with_warnings'] += 1
                        
                        yield record
                        
                        # Log progress periodically
                        if self.stats['rows_read'] % 5000 == 0:
                            logger.info(f"Processed {self.stats['rows_read']} rows...")
                    
                    # Successfully parsed with this encoding
                    break
                    
            except UnicodeDecodeError:
                logger.warning(f"Failed to decode with {encoding}, trying next...")
                continue
        
        logger.info(f"Finished parsing {file_path}: {self.stats}")
    
    def parse_file_to_list(self, file_path: Path,
                           calendar_year: int = None,
                           include_invalid: bool = False) -> List[ParsedRecord]:
        """
        Parse file and return all records as a list
        
        Args:
            file_path: Path to CSV file
            calendar_year: Override calendar year
            include_invalid: Whether to include records with errors
            
        Returns:
            List of ParsedRecord objects
        """
        records = []
        for record in self.parse_file(file_path, calendar_year):
            if include_invalid or record.is_valid:
                records.append(record)
        return records
    
    def get_column_headers(self, file_path: Path) -> List[str]:
        """Read and return column headers from CSV file"""
        file_path = Path(file_path)
        
        encodings = ['utf-8', 'latin-1', 'cp1252']
        
        for encoding in encodings:
            try:
                with open(file_path, 'r', encoding=encoding, newline='') as f:
                    reader = csv.reader(f)
                    headers = next(reader)
                    return [h.strip() for h in headers]
            except (UnicodeDecodeError, StopIteration):
                continue
        
        return []
    
    def validate_file_structure(self, file_path: Path) -> Tuple[bool, List[str]]:
        """
        Validate that CSV file has expected structure
        
        Returns:
            Tuple of (is_valid, list of issues)
        """
        issues = []
        
        try:
            headers = self.get_column_headers(file_path)
        except Exception as e:
            return False, [f"Could not read file: {str(e)}"]
        
        if not headers:
            return False, ["No headers found in file"]
        
        # Check for required columns
        required = ['Thursday', 'Serial No.', 'Grain', 'Destination', 'Pounds']
        missing = []
        
        # Case-insensitive check
        headers_upper = [h.upper() for h in headers]
        for req in required:
            if req.upper() not in headers_upper:
                missing.append(req)
        
        if missing:
            issues.append(f"Missing required columns: {missing}")
        
        # Check for expected column count (should be ~112 columns)
        if len(headers) < 50:
            issues.append(f"Unexpected column count: {len(headers)} (expected ~112)")
        
        is_valid = len(issues) == 0
        return is_valid, issues


class FGISDataTransformer:
    """
    Transform parsed records for database insertion
    Adds calculated fields and region mappings
    """
    
    def __init__(self, config=None):
        from config.settings import default_config
        self.config = config or default_config
    
    def transform_record(self, record: ParsedRecord) -> Dict[str, Any]:
        """
        Transform a parsed record with additional calculated fields
        """
        data = record.data.copy()
        
        # Calculate marketing year if not present
        if not data.get('marketing_year') and data.get('week_ending_date'):
            grain = data.get('grain', 'SOYBEANS')
            data['marketing_year'] = self.config.commodities.get_marketing_year(
                grain, data['week_ending_date']
            )
        
        # Add destination region
        if data.get('destination') and not data.get('destination_region'):
            data['destination_region'] = self.config.regions.get_destination_region(
                data['destination']
            )
        
        # Add port region
        if data.get('port') and not data.get('port_region'):
            # Store as a new field (port_region isn't in the model, but could be added)
            pass
        
        # Ensure metric tons is calculated
        if data.get('pounds') and not data.get('metric_tons'):
            data['metric_tons'] = Decimal(str(data['pounds'])) / Decimal('2204.62')
        
        # Ensure bushels is calculated
        if data.get('pounds') and not data.get('bushels'):
            grain = data.get('grain', '').upper()
            bushel_weight = self.config.commodities.get_bushel_weight(grain)
            data['bushels'] = Decimal(str(data['pounds'])) / Decimal(str(bushel_weight))
        
        return data
    
    def transform_batch(self, records: List[ParsedRecord]) -> List[Dict[str, Any]]:
        """Transform a batch of records"""
        return [self.transform_record(r) for r in records if r.is_valid]

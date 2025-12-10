"""
Unit conversion utilities for commodity data
"""
import pint
import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)

# Initialize pint unit registry
ureg = pint.UnitRegistry()

# Custom commodity conversions
COMMODITY_CONVERSIONS = {
    'SOYBEAN': {
        'bushel_to_mt': 0.0272155,  # 1 bushel soybeans = 0.0272155 metric tons
        'mt_to_bushel': 36.744,     # 1 metric ton = 36.744 bushels
    },
    'CORN': {
        'bushel_to_mt': 0.0254012,  # 1 bushel corn = 0.0254012 metric tons
        'mt_to_bushel': 39.368,     # 1 metric ton = 39.368 bushels
    },
    'WHEAT': {
        'bushel_to_mt': 0.0272155,  # 1 bushel wheat = 0.0272155 metric tons
        'mt_to_bushel': 36.744,
    },
    'SOYOIL': {
        'pound_to_mt': 0.000453592,  # 1 pound = 0.000453592 metric tons
        'mt_to_pound': 2204.62,      # 1 metric ton = 2204.62 pounds
    },
    'BIODIESEL': {
        'gallon_to_mt': 0.00378541,  # 1 gallon biodiesel ≈ 0.00378541 metric tons
        'mt_to_gallon': 264.172,
    },
    'ETHANOL': {
        'gallon_to_mt': 0.00378541,
        'mt_to_gallon': 264.172,
    }
}

class UnitConverter:
    """
    Handles unit conversions for commodity data
    """
    
    def __init__(self, commodity_config):
        """
        Initialize with commodity configuration
        
        Args:
            commodity_config: CommodityConfig instance
        """
        self.commodity_config = commodity_config
    
    def convert(self, value: float, commodity_code: str, 
                from_unit: str, to_unit: str) -> Optional[float]:
        """
        Convert value from one unit to another for a specific commodity
        
        Args:
            value: The value to convert
            commodity_code: Code for the commodity
            from_unit: Source unit (e.g., 'metric_ton', 'bushel', 'gallon')
            to_unit: Target unit
            
        Returns:
            Converted value or None if conversion fails
        """
        try:
            # Normalize unit names
            from_unit = from_unit.lower().replace(' ', '_')
            to_unit = to_unit.lower().replace(' ', '_')
            
            # If units are the same, no conversion needed
            if from_unit == to_unit:
                return value
            
            # Check for commodity-specific conversion
            if commodity_code in COMMODITY_CONVERSIONS:
                conversion_key = f"{from_unit}_to_{to_unit}"
                if conversion_key in COMMODITY_CONVERSIONS[commodity_code]:
                    factor = COMMODITY_CONVERSIONS[commodity_code][conversion_key]
                    return value * factor
            
            # Try generic pint conversion
            try:
                quantity = value * ureg(from_unit)
                converted = quantity.to(to_unit)
                return converted.magnitude
            except:
                pass
            
            # If all else fails, check commodity config
            commodity_info = self.commodity_config.get_commodity_info(commodity_code)
            if commodity_info:
                if from_unit == 'metric_ton' and to_unit == commodity_info['unit_preference']:
                    return value * commodity_info['metric_ton_conversion']
                elif from_unit == commodity_info['unit_preference'] and to_unit == 'metric_ton':
                    return value / commodity_info['metric_ton_conversion']
            
            logger.warning(f"No conversion available for {commodity_code}: {from_unit} to {to_unit}")
            return None
            
        except Exception as e:
            logger.error(f"Conversion error: {e}")
            return None
    
    def convert_to_preferred_unit(
        self, value: float, commodity_code: str, current_unit: str
        ) -> Optional[float]:
        """
        Convert value to the preferred unit for this commodity
        
        Args:
            value: The value to convert
            commodity_code: Code for the commodity
            current_unit: Current unit of the value
            
        Returns:
            Value in preferred unit or None if conversion fails
        """
        commodity_info = self.commodity_config.get_commodity_info(commodity_code)
        if not commodity_info:
            return None
        
        preferred_unit = commodity_info['unit_preference']
        return self.convert(value, commodity_code, current_unit, preferred_unit)
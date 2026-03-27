"""
Configuration loader and reference table seeder for the oilseed crush engine.

Reads config/oilseed_crush_params.yaml and provides OilseedParams dataclasses.
Can upsert parameters into reference.oilseed_crush_params.
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).resolve().parents[3] / "config" / "oilseed_crush_params.yaml"


@dataclass
class OilseedParams:
    """Parameters for one oilseed in the crush margin model."""
    oilseed_code: str
    oilseed_name: str
    oil_yield_pct: float
    meal_yield_pct: float
    hull_yield_pct: float
    processing_cost_per_unit: float
    seed_unit: str
    seed_lbs_per_unit: float
    oil_price_source: str
    meal_price_source: str
    seed_price_source: str
    my_start_month: int
    has_nass_monthly: bool
    nass_source: Optional[str] = None
    nass_attribute: Optional[str] = None
    seed_price_divisor: float = 1.0  # Converts raw settlement to $/seed_unit
    usda_annual_crush: Optional[float] = None
    usda_annual_unit: Optional[str] = None
    capacity_annual_thou_tons: Optional[float] = None
    seasonal_pattern: Optional[List[float]] = None
    # Regression coefficients (populated after calibration)
    reg_intercept: Optional[float] = None
    reg_margin_coeff: Optional[float] = None
    reg_margin_lag_coeff: Optional[float] = None
    reg_r_squared: Optional[float] = None


def load_config(config_path: Path = None) -> Dict[str, OilseedParams]:
    """Load oilseed parameters from YAML config file."""
    path = config_path or CONFIG_PATH
    logger.info(f"Loading crush config from {path}")

    with open(path, "r") as f:
        raw = yaml.safe_load(f)

    params = {}
    for code, values in raw.get("oilseeds", {}).items():
        params[code] = OilseedParams(
            oilseed_code=code,
            oilseed_name=values.get("oilseed_name", code.title()),
            oil_yield_pct=values["oil_yield_pct"],
            meal_yield_pct=values["meal_yield_pct"],
            hull_yield_pct=values.get("hull_yield_pct", 0),
            processing_cost_per_unit=values["processing_cost_per_unit"],
            seed_unit=values["seed_unit"],
            seed_lbs_per_unit=values["seed_lbs_per_unit"],
            oil_price_source=values.get("oil_price_source", ""),
            meal_price_source=values.get("meal_price_source", ""),
            seed_price_source=values.get("seed_price_source", ""),
            seed_price_divisor=values.get("seed_price_divisor", 1.0),
            my_start_month=values.get("my_start_month", 9),
            has_nass_monthly=values.get("has_nass_monthly", False),
            nass_source=values.get("nass_source"),
            nass_attribute=values.get("nass_attribute"),
            usda_annual_crush=values.get("usda_annual_crush"),
            usda_annual_unit=values.get("usda_annual_unit"),
            capacity_annual_thou_tons=values.get("capacity_annual_thou_tons"),
            seasonal_pattern=values.get("seasonal_pattern"),
        )

    logger.info(f"Loaded {len(params)} oilseed configurations")
    return params


def seed_reference_table(conn, params: Dict[str, OilseedParams]):
    """Upsert oilseed parameters into reference.oilseed_crush_params."""
    sql = """
        INSERT INTO reference.oilseed_crush_params (
            oilseed_code, oilseed_name,
            oil_yield_pct, meal_yield_pct, hull_yield_pct,
            processing_cost_per_unit, seed_unit, seed_lbs_per_unit,
            oil_price_source, meal_price_source, seed_price_source,
            my_start_month, has_nass_monthly, nass_source, nass_attribute,
            usda_annual_crush, usda_annual_unit,
            capacity_annual_thou_tons, seasonal_pattern,
            updated_at
        ) VALUES (
            %(oilseed_code)s, %(oilseed_name)s,
            %(oil_yield_pct)s, %(meal_yield_pct)s, %(hull_yield_pct)s,
            %(processing_cost_per_unit)s, %(seed_unit)s, %(seed_lbs_per_unit)s,
            %(oil_price_source)s, %(meal_price_source)s, %(seed_price_source)s,
            %(my_start_month)s, %(has_nass_monthly)s, %(nass_source)s, %(nass_attribute)s,
            %(usda_annual_crush)s, %(usda_annual_unit)s,
            %(capacity_annual_thou_tons)s, %(seasonal_pattern)s,
            NOW()
        )
        ON CONFLICT (oilseed_code) DO UPDATE SET
            oilseed_name = EXCLUDED.oilseed_name,
            oil_yield_pct = EXCLUDED.oil_yield_pct,
            meal_yield_pct = EXCLUDED.meal_yield_pct,
            hull_yield_pct = EXCLUDED.hull_yield_pct,
            processing_cost_per_unit = EXCLUDED.processing_cost_per_unit,
            seed_unit = EXCLUDED.seed_unit,
            seed_lbs_per_unit = EXCLUDED.seed_lbs_per_unit,
            oil_price_source = EXCLUDED.oil_price_source,
            meal_price_source = EXCLUDED.meal_price_source,
            seed_price_source = EXCLUDED.seed_price_source,
            my_start_month = EXCLUDED.my_start_month,
            has_nass_monthly = EXCLUDED.has_nass_monthly,
            nass_source = EXCLUDED.nass_source,
            nass_attribute = EXCLUDED.nass_attribute,
            usda_annual_crush = EXCLUDED.usda_annual_crush,
            usda_annual_unit = EXCLUDED.usda_annual_unit,
            capacity_annual_thou_tons = EXCLUDED.capacity_annual_thou_tons,
            seasonal_pattern = EXCLUDED.seasonal_pattern,
            updated_at = NOW()
    """
    cur = conn.cursor()
    count = 0
    for p in params.values():
        cur.execute(sql, {
            'oilseed_code': p.oilseed_code,
            'oilseed_name': p.oilseed_name,
            'oil_yield_pct': p.oil_yield_pct,
            'meal_yield_pct': p.meal_yield_pct,
            'hull_yield_pct': p.hull_yield_pct,
            'processing_cost_per_unit': p.processing_cost_per_unit,
            'seed_unit': p.seed_unit,
            'seed_lbs_per_unit': p.seed_lbs_per_unit,
            'oil_price_source': p.oil_price_source,
            'meal_price_source': p.meal_price_source,
            'seed_price_source': p.seed_price_source,
            'my_start_month': p.my_start_month,
            'has_nass_monthly': p.has_nass_monthly,
            'nass_source': p.nass_source,
            'nass_attribute': p.nass_attribute,
            'usda_annual_crush': p.usda_annual_crush,
            'usda_annual_unit': p.usda_annual_unit,
            'capacity_annual_thou_tons': p.capacity_annual_thou_tons,
            'seasonal_pattern': p.seasonal_pattern,
        })
        count += 1
    conn.commit()
    logger.info(f"Seeded {count} oilseed parameters into reference table")

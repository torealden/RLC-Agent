"""
Feedstock Allocation Engine — The "Junior Buyer" Algorithm

Allocates feedstock to every US biofuel facility each month using a
constrained greedy optimization that mimics how a real procurement
manager thinks:

    1. PATHWAY GATE    — Can this plant use this feedstock? (EPA approval)
    2. EQUIPMENT GATE  — Can this technology process it? (HEFA vs transester)
    3. CAPACITY NEED   — How much feedstock does this plant need this month?
    4. SUPPLY GATE     — Is there enough available in the region?
    5. ECONOMICS       — Among eligible feedstocks, which maximizes margin?
    6. BEHAVIORAL      — Diversification, switching cost, contract lock-in

The engine runs month-by-month, tracking cumulative supply consumption
so that early-allocated plants reduce availability for later ones.

Usage:
    python -m src.engines.feedstock_allocation.allocator --period 2025-01
    python -m src.engines.feedstock_allocation.allocator --range 2024-01 2024-12
    python -m src.engines.feedstock_allocation.allocator --period 2025-01 --scenario high_sbo
"""

import argparse
import logging
import sys
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
load_dotenv()

from src.engines.feedstock_allocation.margin_model import (
    CreditResolver,
    CreditStack,
    FeedstockCost,
    MarginCalculator,
    MarginResult,
    TallowSplitCalculator,
    CONVERSION_RATES,
    RIN_EQUIVALENCE,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')


# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURATION — Behavioral parameters (tunable during calibration)
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class AllocationConfig:
    """Tunable parameters for the allocation engine."""

    # --- Utilization rates ---
    default_utilization: float = 0.85       # 85% of nameplate capacity
    new_plant_ramp_months: int = 12         # Months to reach full utilization
    new_plant_initial_util: float = 0.40    # Starting utilization for new plants
    idle_utilization: float = 0.0

    # --- Behavioral realism ---
    max_single_feedstock_pct: float = 0.80  # No plant runs >80% of one feedstock
    max_monthly_switch_pct: float = 0.20    # Max 20% feedstock mix change MoM
    contract_share: float = 0.60            # 60% of feedstock under term contracts
    contract_feedstock: str = None           # If set, contract is locked to this code

    # --- Supply constraints ---
    regional_supply_buffer: float = 0.10    # Reserve 10% of regional supply as buffer

    # --- Default prices (used when no price data available) ---
    default_ulsd_price: float = 2.50        # $/gal
    default_b100_price: float = 4.50        # $/gal
    default_rin_price_cents: float = 150.0   # cents/RIN
    default_lcfs_per_gal: float = 0.50      # $/gal (CA only)

    # --- Processing costs ---
    processing_cost_hefa: float = 0.40
    processing_cost_transester: float = 0.35
    processing_cost_copro: float = 0.25


# ═══════════════════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class Facility:
    """A biofuel production facility."""
    facility_id: int
    company: str
    facility_name: str
    state: str
    padd: str
    fuel_type: str             # biodiesel, renewable_diesel, saf, coprocessing
    technology: str            # hefa, transesterification, coprocessing
    nameplate_mmgy: float
    status: str
    year_online: int = None
    eligible_feedstocks: list = field(default_factory=list)
    primary_feedstock: str = None
    feedstock_mix: str = None


@dataclass
class RegionalSupply:
    """Available feedstock supply in a region for one month."""
    feedstock_code: str
    region: str                # PADD code
    available_mil_lbs: float   # Net available for biofuel
    price_per_lb: float        # Delivered cost in region
    consumed_mil_lbs: float = 0.0  # Running total consumed by allocation

    @property
    def remaining_mil_lbs(self) -> float:
        return max(0.0, self.available_mil_lbs - self.consumed_mil_lbs)


@dataclass
class AllocationResult:
    """One plant's feedstock allocation for one month."""
    facility_id: int
    fuel_type: str
    feedstock_code: str
    allocated_mil_lbs: float
    allocated_mil_gal: float
    pct_of_facility: float
    feedstock_cost_lb: float
    margin_per_gal: float
    margin_rank: int
    constraint_binding: str    # 'none', 'supply', 'pathway', 'diversification', 'switching_cost'


# ═══════════════════════════════════════════════════════════════════════════
# THE ALLOCATOR
# ═══════════════════════════════════════════════════════════════════════════

class FeedstockAllocator:
    """
    Monthly feedstock allocation engine.

    For each month:
    1. Load all active facilities
    2. Load regional feedstock supply & prices
    3. Calculate margins for every (facility, feedstock) pair
    4. Allocate greedily: highest-margin facility-feedstock pairs first
    5. Apply behavioral constraints
    6. Write results to gold.feedstock_allocation
    """

    def __init__(self, config: AllocationConfig = None):
        self.config = config or AllocationConfig()
        self.margin_calc = MarginCalculator()
        self.credit_resolver = CreditResolver()

    def load_facilities(self, period: date) -> list:
        """Load all active facilities for the given period."""
        from src.services.database.db_config import get_connection

        facilities = []
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT facility_id, company, facility_name, state, padd,
                           fuel_type, technology, nameplate_mmgy, status,
                           year_online, eligible_feedstocks, primary_feedstock,
                           feedstock_mix
                    FROM reference.biofuel_facilities
                    WHERE status IN ('operating', 'under_construction')
                      AND nameplate_mmgy > 0
                    ORDER BY nameplate_mmgy DESC
                """)
                for row in cur.fetchall():
                    # Skip plants not yet online
                    yr = row['year_online']
                    if yr and yr > period.year:
                        continue

                    # Expand BFT → EBFT/IBFT in eligible feedstocks
                    eligible = list(row['eligible_feedstocks'] or [])
                    if 'BFT' in eligible:
                        if 'EBFT' not in eligible:
                            eligible.append('EBFT')
                        if 'IBFT' not in eligible:
                            eligible.append('IBFT')

                    f = Facility(
                        facility_id=row['facility_id'],
                        company=row['company'],
                        facility_name=row['facility_name'],
                        state=row['state'] or '',
                        padd=row['padd'] or 'PADD2',
                        fuel_type=row['fuel_type'],
                        technology=row['technology'] or self._infer_technology(row['fuel_type']),
                        nameplate_mmgy=float(row['nameplate_mmgy']),
                        status=row['status'],
                        year_online=yr,
                        eligible_feedstocks=eligible,
                        primary_feedstock=row['primary_feedstock'],
                        feedstock_mix=row['feedstock_mix'],
                    )
                    facilities.append(f)

        logger.info(f"  Loaded {len(facilities)} active facilities for {period}")
        return facilities

    def _infer_technology(self, fuel_type: str) -> str:
        """Infer technology from fuel type if not specified."""
        return {
            'biodiesel': 'transesterification',
            'renewable_diesel': 'hefa',
            'saf': 'hefa',
            'coprocessing': 'coprocessing',
        }.get(fuel_type, 'hefa')

    def get_utilization_rate(self, facility: Facility, period: date) -> float:
        """Calculate utilization rate accounting for ramp-up."""
        if facility.status == 'idle':
            return self.config.idle_utilization

        if facility.year_online:
            months_online = (period.year - facility.year_online) * 12 + period.month
            if months_online <= 0:
                return 0.0
            if months_online < self.config.new_plant_ramp_months:
                # Linear ramp from initial to default
                ramp_pct = months_online / self.config.new_plant_ramp_months
                return (self.config.new_plant_initial_util +
                        ramp_pct * (self.config.default_utilization - self.config.new_plant_initial_util))

        return self.config.default_utilization

    def get_monthly_feedstock_need(self, facility: Facility, period: date) -> float:
        """
        Calculate total feedstock needed (million lbs) for this month.

        If balance-sheet production forecasts have been loaded (via
        distribute_production_forecasts), uses the facility's implied
        monthly gallons. Otherwise falls back to nameplate * utilization.
        """
        # Prefer balance-sheet forecast if available
        implied_gal = getattr(facility, '_implied_monthly_gal', None)
        if implied_gal is not None and implied_gal > 0:
            monthly_gal = implied_gal
        else:
            # Fallback: capacity-based estimate
            util = self.get_utilization_rate(facility, period)
            monthly_gal = facility.nameplate_mmgy * util / 12.0

        # Get average lbs/gal for this technology
        tech_rates = CONVERSION_RATES.get(facility.technology, {})
        avg_lbs_per_gal = sum(tech_rates.values()) / len(tech_rates) if tech_rates else 8.0

        return monthly_gal * avg_lbs_per_gal  # Million lbs/month

    def load_production_forecasts(self, period: date) -> dict:
        """
        Load analyst's monthly production forecasts from balance sheets.

        Reads silver.fuel_production_forecast for the given month.
        Returns dict: {fuel_type: production_mmgal}
        """
        from src.services.database.db_config import get_connection

        forecasts = {}
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT fuel_type, production_mmgal
                        FROM silver.fuel_production_forecast
                        WHERE period = %s AND production_mmgal IS NOT NULL
                    """, (period,))
                    for row in cur.fetchall():
                        forecasts[row[0]] = float(row[1])
        except Exception as e:
            logger.debug(f"  No production forecasts available: {e}")

        if forecasts:
            logger.info(f"  Loaded production forecasts: {forecasts}")
        return forecasts

    def distribute_production_forecasts(self, facilities: list, forecasts: dict,
                                        period: date):
        """
        Distribute national production totals to individual facilities
        proportionally by effective capacity (nameplate * utilization).

        Sets facility._implied_monthly_gal on each facility object.
        """
        if not forecasts:
            return

        # Group facilities by fuel_type and sum effective capacity
        fuel_type_capacity = defaultdict(float)
        for fac in facilities:
            util = self.get_utilization_rate(fac, period)
            eff_cap = fac.nameplate_mmgy * util
            fac._effective_capacity = eff_cap
            fuel_type_capacity[fac.fuel_type] += eff_cap

        # Distribute national production to each facility by share
        distributed_count = 0
        for fac in facilities:
            ft = fac.fuel_type
            if ft in forecasts and fuel_type_capacity[ft] > 0:
                share = fac._effective_capacity / fuel_type_capacity[ft]
                fac._implied_monthly_gal = forecasts[ft] * share
                distributed_count += 1
            else:
                fac._implied_monthly_gal = None

        if distributed_count:
            logger.info(f"  Distributed forecasts to {distributed_count} facilities")

    def load_feedstock_supply(self, period: date) -> dict:
        """
        Load or estimate regional feedstock supply.

        Returns dict: {(feedstock_code, padd): RegionalSupply}

        For now, uses estimated national supply distributed proportionally
        to PADD-level plant capacity. This will be refined as real supply
        data is loaded.
        """
        from src.services.database.db_config import get_connection

        supply = {}

        # Try to load from silver.feedstock_supply first
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT feedstock_code, region, net_available_biofuel, avg_price_per_lb
                    FROM silver.feedstock_supply
                    WHERE period = %s
                """, (period,))
                rows = cur.fetchall()

                if rows:
                    for row in rows:
                        key = (row['feedstock_code'], row['region'])
                        supply[key] = RegionalSupply(
                            feedstock_code=row['feedstock_code'],
                            region=row['region'],
                            available_mil_lbs=float(row['net_available_biofuel'] or 0),
                            price_per_lb=float(row['avg_price_per_lb'] or 0),
                        )
                    logger.info(f"  Loaded {len(supply)} supply records from database")
                    return supply

        # Fallback: estimate supply from EIA feedstock actuals + distribute by PADD
        logger.info("  No supply data in DB — using estimated national supply")
        supply = self._estimate_supply(period)
        return supply

    def _load_real_prices(self, period: date) -> dict:
        """
        Load actual feedstock prices from bronze.feedstock_prices for the period.
        Returns dict: {feedstock_code: price_per_lb} using the closest available date.
        """
        from src.services.database.db_config import get_connection

        prices = {}
        with get_connection() as conn:
            with conn.cursor() as cur:
                # Get the most recent price for each feedstock near this period
                # Map our regions to feedstock codes for US-relevant prices
                region_priority = {
                    'SBO': ['central_il', 'central_il_rbd', 'central_il_rbd_wk', 'us_gulf', 'cbot_futures'],
                    'CO': ['central_us', 'canada_cnf', 'los_angeles', 'canada_cnf_raw'],
                    'DCO': ['il_wi', 'west_coast'],
                    'EBFT': ['chicago', 'west_coast'],       # Edible = packer/BFT grade
                    'IBFT': ['chicago', 'central_us'],       # Inedible = renderer grade
                    'BFT': ['chicago', 'west_coast'],        # Legacy fallback
                    'CWG': ['missouri_river', 'west_coast'],
                    'PF': ['southeast', 'west_coast'],
                    'YG': ['il_wi', 'los_angeles'],
                    'UCO': ['il_wi', 'socal'],
                }

                for fs_code, regions in region_priority.items():
                    for region in regions:
                        cur.execute("""
                            SELECT price_per_lb FROM bronze.feedstock_prices
                            WHERE feedstock_code = %s AND region = %s
                              AND price_date <= %s AND price_per_lb > 0
                            ORDER BY price_date DESC LIMIT 1
                        """, (fs_code, region, period))
                        row = cur.fetchone()
                        if row and row['price_per_lb']:
                            val = float(row['price_per_lb'])
                            # Some values are in cents (>1), convert to $/lb
                            if val > 1.0:
                                val = val / 100.0
                            prices[fs_code] = val
                            break

        # Derive EBFT/IBFT from BFT if no grade-specific prices exist yet
        # BFT (packer Chicago) ≈ edible grade; inedible trades at ~85% of edible
        if 'EBFT' not in prices and 'BFT' in prices:
            prices['EBFT'] = prices['BFT']
            logger.info(f"    EBFT: derived from BFT ${prices['BFT']:.4f}/lb")
        if 'IBFT' not in prices:
            base = prices.get('EBFT', prices.get('BFT', 0.42))
            prices['IBFT'] = round(base * 0.85, 6)
            logger.info(f"    IBFT: estimated at 85% of edible ${prices['IBFT']:.4f}/lb")

        return prices

    def _estimate_supply(self, period: date) -> dict:
        """
        Generate estimated feedstock supply by PADD using real prices
        from the database where available, falling back to defaults.
        """
        # Approximate annual US feedstock availability for biofuel (million lbs)
        annual_supply_mil_lbs = {
            'SBO':  12000,   # ~24B lbs US production, ~50% to biofuel
            'CO':    1500,   # Canola oil (imports + domestic)
            'DCO':   4000,   # Distillers corn oil from ethanol plants
            'EBFT':  1400,   # Edible tallow available for BBD (~3.5B production, ~40% to BBD)
            'IBFT':  2600,   # Inedible tallow available for BBD (~3.0B production, ~65% to BBD)
            'CWG':   1500,   # Choice white grease
            'PF':    1200,   # Poultry fat
            'YG':    2500,   # Yellow grease
            'UCO':   3000,   # UCO (domestic + imports — growing fast)
        }

        padd_weights = {
            'PADD1': 0.08,
            'PADD2': 0.45,
            'PADD3': 0.30,
            'PADD4': 0.05,
            'PADD5': 0.12,
        }

        # Try to load real prices from database
        real_prices = self._load_real_prices(period)

        default_prices = {
            'SBO': 0.45, 'CO': 0.52, 'DCO': 0.38,
            'EBFT': 0.44, 'IBFT': 0.38, 'BFT': 0.42,
            'CWG': 0.35, 'PF': 0.38, 'YG': 0.32, 'UCO': 0.40,
        }

        # Use real prices where available, defaults otherwise
        final_prices = {}
        for fs_code in annual_supply_mil_lbs:
            if fs_code in real_prices:
                final_prices[fs_code] = real_prices[fs_code]
                logger.info(f"    {fs_code}: ${real_prices[fs_code]:.4f}/lb (from DB)")
            else:
                final_prices[fs_code] = default_prices.get(fs_code, 0.40)
                logger.info(f"    {fs_code}: ${final_prices[fs_code]:.4f}/lb (default)")

        supply = {}
        for fs_code, annual in annual_supply_mil_lbs.items():
            monthly = annual / 12.0
            for padd, weight in padd_weights.items():
                regional_avail = monthly * weight * (1.0 - self.config.regional_supply_buffer)
                key = (fs_code, padd)
                supply[key] = RegionalSupply(
                    feedstock_code=fs_code,
                    region=padd,
                    available_mil_lbs=regional_avail,
                    price_per_lb=final_prices.get(fs_code, 0.40),
                )

        return supply

    def load_prices(self, period: date) -> dict:
        """
        Load credit, fuel, and feedstock prices for the period from database.
        Falls back to defaults where real data is unavailable.
        """
        from src.services.database.db_config import get_connection

        prices = {
            'rin_d4_cents': self.config.default_rin_price_cents,
            'lcfs_per_gal': self.config.default_lcfs_per_gal,
            'ulsd': self.config.default_ulsd_price,
            'b100': self.config.default_b100_price,
        }

        with get_connection() as conn:
            with conn.cursor() as cur:
                # Credit prices — try weekly first (more granular), then monthly
                for freq in ['weekly', 'monthly']:
                    cur.execute("""
                        SELECT d4_rin, lcfs_ca
                        FROM bronze.credit_prices
                        WHERE price_date <= %s AND frequency = %s
                        ORDER BY price_date DESC LIMIT 1
                    """, (period, freq))
                    row = cur.fetchone()
                    if row:
                        if row['d4_rin'] and prices['rin_d4_cents'] == self.config.default_rin_price_cents:
                            prices['rin_d4_cents'] = float(row['d4_rin'])
                        if row['lcfs_ca'] and prices['lcfs_per_gal'] == self.config.default_lcfs_per_gal:
                            prices['lcfs_per_gal'] = float(row['lcfs_ca'])

                # Fuel prices — BD regional and RD
                cur.execute("""
                    SELECT b100_upper_midwest, rd_california
                    FROM bronze.fuel_prices
                    WHERE price_date <= %s
                    ORDER BY price_date DESC LIMIT 1
                """, (period,))
                row = cur.fetchone()
                if row:
                    if row['b100_upper_midwest']:
                        prices['b100'] = float(row['b100_upper_midwest'])
                    if row['rd_california']:
                        prices['rd_ca'] = float(row['rd_california'])

        logger.info(f"  Prices: D4 RIN={prices['rin_d4_cents']:.1f}c, "
                    f"LCFS=${prices['lcfs_per_gal']:.2f}, "
                    f"B100=${prices['b100']:.2f}/gal, "
                    f"ULSD=${prices['ulsd']:.2f}/gal")

        return prices

    def load_eia_tallow(self, period: date) -> Optional[float]:
        """
        Load EIA Form 819 total tallow consumption for the period.
        This is the guardrail — EBFT + IBFT must reconcile to this total.

        Returns million lbs, or None if no EIA data for this period.
        """
        from src.services.database.db_config import get_connection

        with get_connection() as conn:
            with conn.cursor() as cur:
                # EIA feedstock data uses "Tallow" as the feedstock name
                cur.execute("""
                    SELECT quantity_mil_lbs
                    FROM bronze.eia_feedstock_monthly
                    WHERE LOWER(feedstock_name) LIKE '%%tallow%%'
                      AND year = %s AND month = %s
                      AND source_sheet IN ('table_2b', 'table_2c')
                      AND is_withheld = FALSE
                """, (period.year, period.month))
                rows = cur.fetchall()

                if rows:
                    total = sum(float(r['quantity_mil_lbs'] or 0) for r in rows)
                    logger.info(f"  EIA tallow total: {total:.1f} mil lbs ({len(rows)} records)")
                    return total

        logger.info(f"  No EIA tallow data for {period} — will use supply estimates")
        return None

    def run_tallow_split(self, period: date, prices: dict, supply: dict) -> Optional[dict]:
        """
        Run the tallow grade split and update supply dict with EBFT/IBFT volumes.

        If EIA data exists, uses it as the guardrail and allocates between grades
        based on economic pull. If no EIA data, uses default supply estimates.

        Returns the tallow split result dict, or None if using defaults.
        """
        eia_total = self.load_eia_tallow(period)

        if eia_total is None or eia_total <= 0:
            return None  # Use default supply estimates (EBFT/IBFT already in supply dict)

        # Run economic split
        tallow_split = TallowSplitCalculator()
        result = tallow_split.allocate_tallow(
            eia_total_mil_lbs=eia_total,
            period=period,
            fuel_price=prices.get('ulsd', self.config.default_ulsd_price),
            rin_price_cents=prices.get('rin_d4_cents', self.config.default_rin_price_cents),
            lcfs_credit_per_gal=prices.get('lcfs_per_gal', 0.0),
            pathway='hefa',
            fuel_type='renewable_diesel',
        )

        # Update supply dict to reflect EIA-guardrailed volumes
        # Scale PADD-level supply proportionally so total matches EIA
        padd_weights = {
            'PADD1': 0.08, 'PADD2': 0.45, 'PADD3': 0.30,
            'PADD4': 0.05, 'PADD5': 0.12,
        }

        for grade in ['ebft', 'ibft']:
            grade_code = grade.upper()
            monthly_total = result[grade]['allocated_mil_lbs']

            for padd, weight in padd_weights.items():
                key = (grade_code, padd)
                regional_avail = monthly_total * weight * (1.0 - self.config.regional_supply_buffer)

                if key in supply:
                    supply[key].available_mil_lbs = regional_avail
                else:
                    # Get price from supply or defaults
                    price = result[grade]['market_price_per_lb']
                    supply[key] = RegionalSupply(
                        feedstock_code=grade_code,
                        region=padd,
                        available_mil_lbs=regional_avail,
                        price_per_lb=price,
                    )

        ebft_data = result['ebft']
        ibft_data = result['ibft']
        logger.info(
            f"  Tallow split: EBFT {ebft_data['allocated_mil_lbs']:.1f} mil lbs "
            f"({ebft_data['allocated_pct']:.0f}%), "
            f"IBFT {ibft_data['allocated_mil_lbs']:.1f} mil lbs "
            f"({ibft_data['allocated_pct']:.0f}%) "
            f"[EIA total: {eia_total:.1f}]"
        )

        return result

    def allocate_month(self, period: date, scenario: str = 'base') -> list:
        """
        Run the allocation engine for one month.

        Returns list of AllocationResult for every (facility, feedstock) pair.
        """
        logger.info(f"=== ALLOCATING {period.strftime('%Y-%m')} (scenario: {scenario}) ===")

        # 1. Load data
        facilities = self.load_facilities(period)
        supply = self.load_feedstock_supply(period)
        prices = self.load_prices(period)

        # 1a. Run tallow grade split (EIA guardrail)
        tallow_result = self.run_tallow_split(period, prices, supply)
        self._last_tallow_result = tallow_result  # Stored for save_tallow_split

        if not facilities:
            logger.warning("  No active facilities — skipping")
            return []

        # 1b. Load balance-sheet production forecasts and distribute to facilities
        forecasts = self.load_production_forecasts(period)
        self.distribute_production_forecasts(facilities, forecasts, period)

        # 2. Calculate margins for every (facility, feedstock) pair
        all_margins = []

        for fac in facilities:
            # Determine fuel price
            if fac.fuel_type == 'biodiesel':
                fuel_price = prices['b100']
            else:
                fuel_price = prices['ulsd']

            # Determine LCFS applicability
            lcfs = prices['lcfs_per_gal'] if fac.padd == 'PADD5' else 0.0

            # Build credit stack
            credits = self.credit_resolver.build_credit_stack(
                calc_date=period,
                fuel_type=fac.fuel_type,
                rin_price_cents=prices['rin_d4_cents'],
                lcfs_credit_per_gal=lcfs,
                destination_state=fac.state if fac.state else None,
            )

            # Get eligible feedstocks
            tech_rates = CONVERSION_RATES.get(fac.technology, {})
            eligible = fac.eligible_feedstocks if fac.eligible_feedstocks else list(tech_rates.keys())

            # Calculate margin for each eligible feedstock
            for fs_code in eligible:
                if fs_code == 'OTHER':
                    continue

                # Get supply/price for this feedstock in this PADD
                supply_key = (fs_code, fac.padd)
                regional = supply.get(supply_key)
                if not regional or regional.available_mil_lbs <= 0:
                    continue

                feedstock = FeedstockCost(
                    feedstock_code=fs_code,
                    price_per_lb=regional.price_per_lb,
                    freight_per_lb=0.02,  # Default intra-PADD freight
                )

                margin = self.margin_calc.calculate(
                    facility_id=fac.facility_id,
                    fuel_type=fac.fuel_type,
                    technology=fac.technology,
                    feedstock=feedstock,
                    fuel_price=fuel_price,
                    credits=credits,
                    period=period,
                )

                all_margins.append((fac, fs_code, margin, regional))

        # 3. Sort by margin (best first) — this is the greedy ordering
        all_margins.sort(key=lambda x: x[2].margin_per_gal, reverse=True)

        # 4. Allocate greedily with constraints
        facility_allocated = defaultdict(float)       # facility_id → total mil_lbs allocated
        facility_feedstock_pct = defaultdict(dict)    # facility_id → {fs_code: pct}
        facility_need = {}                            # facility_id → total need mil_lbs

        for fac in facilities:
            facility_need[fac.facility_id] = self.get_monthly_feedstock_need(fac, period)

        results = []

        for fac, fs_code, margin, regional in all_margins:
            fid = fac.facility_id
            need = facility_need.get(fid, 0)
            already = facility_allocated[fid]
            remaining_need = need - already

            if remaining_need <= 0.01:
                continue  # Facility fully supplied

            # --- SUPPLY CONSTRAINT ---
            available = regional.remaining_mil_lbs
            if available <= 0.01:
                continue

            # --- DIVERSIFICATION CONSTRAINT ---
            current_pcts = facility_feedstock_pct[fid]
            if fs_code in current_pcts and current_pcts[fs_code] >= self.config.max_single_feedstock_pct:
                constraint = 'diversification'
                continue

            # How much can we allocate?
            # Limited by: remaining need, available supply, diversification cap
            max_from_diversification = need * self.config.max_single_feedstock_pct - \
                (current_pcts.get(fs_code, 0) * need)
            allocate = min(remaining_need, available, max(max_from_diversification, 0.01))

            if allocate <= 0.001:
                continue

            # Get lbs_per_gal for conversion
            lbs_per_gal = self.margin_calc.get_conversion_rate(fac.technology, fs_code)
            gal_produced = allocate / lbs_per_gal if lbs_per_gal > 0 else 0

            # Record allocation
            facility_allocated[fid] += allocate
            regional.consumed_mil_lbs += allocate

            # Update percentages
            new_total = facility_allocated[fid]
            for existing_fs in current_pcts:
                # Recalculate as pct of new total
                pass
            current_pcts[fs_code] = current_pcts.get(fs_code, 0) + (allocate / need if need > 0 else 0)

            # Determine binding constraint
            constraint = 'none'
            if allocate < remaining_need and available <= allocate:
                constraint = 'supply'
            elif allocate < remaining_need and max_from_diversification <= allocate:
                constraint = 'diversification'

            results.append(AllocationResult(
                facility_id=fid,
                fuel_type=fac.fuel_type,
                feedstock_code=fs_code,
                allocated_mil_lbs=allocate,
                allocated_mil_gal=gal_produced,
                pct_of_facility=allocate / need if need > 0 else 0,
                feedstock_cost_lb=margin.feedstock_cost_per_lb,
                margin_per_gal=margin.margin_per_gal,
                margin_rank=0,
                constraint_binding=constraint,
            ))

        # 5. Assign margin ranks per facility
        fac_results = defaultdict(list)
        for r in results:
            fac_results[r.facility_id].append(r)
        for fid, fac_res in fac_results.items():
            fac_res.sort(key=lambda x: x.margin_per_gal, reverse=True)
            for i, r in enumerate(fac_res):
                r.margin_rank = i + 1

        # Summary
        total_lbs = sum(r.allocated_mil_lbs for r in results)
        total_gal = sum(r.allocated_mil_gal for r in results)
        fac_count = len(set(r.facility_id for r in results))
        logger.info(f"  Allocated {total_lbs:,.0f} mil lbs ({total_gal:,.0f} mil gal) across {fac_count} facilities")

        # Feedstock breakdown
        fs_totals = defaultdict(float)
        for r in results:
            fs_totals[r.feedstock_code] += r.allocated_mil_lbs
        for fs, total in sorted(fs_totals.items(), key=lambda x: -x[1]):
            pct = total / total_lbs * 100 if total_lbs > 0 else 0
            logger.info(f"    {fs}: {total:,.0f} mil lbs ({pct:.1f}%)")

        return results

    def save_results(self, period: date, results: list, scenario: str = 'base'):
        """Write allocation results to gold.feedstock_allocation."""
        from src.services.database.db_config import get_connection

        run_id = str(uuid.uuid4())

        with get_connection() as conn:
            with conn.cursor() as cur:
                loaded = 0
                for r in results:
                    try:
                        cur.execute("SAVEPOINT sp_alloc")
                        cur.execute("""
                            INSERT INTO gold.feedstock_allocation
                                (period, run_id, scenario, facility_id, fuel_type,
                                 feedstock_code, allocated_mil_lbs, allocated_mil_gal,
                                 pct_of_facility, feedstock_cost_lb, margin_per_gal,
                                 margin_rank, constraint_binding)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            period, run_id, scenario, r.facility_id, r.fuel_type,
                            r.feedstock_code, r.allocated_mil_lbs, r.allocated_mil_gal,
                            r.pct_of_facility, r.feedstock_cost_lb, r.margin_per_gal,
                            r.margin_rank, r.constraint_binding,
                        ))
                        cur.execute("RELEASE SAVEPOINT sp_alloc")
                        loaded += 1
                    except Exception as e:
                        cur.execute("ROLLBACK TO SAVEPOINT sp_alloc")
                        logger.warning(f"  Save error: {e}")

                conn.commit()
                logger.info(f"  Saved {loaded} allocation records (run_id: {run_id[:8]}...)")

        return run_id

    def save_tallow_split(self, period: date, tallow_result: dict, run_id: str):
        """Write tallow grade split results to gold.tallow_allocation_detail and silver.tallow_implied_value."""
        from src.services.database.db_config import get_connection

        with get_connection() as conn:
            with conn.cursor() as cur:
                # Save implied values to silver
                for grade_key in ['ebft_iv', 'ibft_iv']:
                    iv = tallow_result[grade_key]
                    grade_data = tallow_result[grade_key.replace('_iv', '')]
                    try:
                        cur.execute("SAVEPOINT sp_tiv")
                        cur.execute("""
                            INSERT INTO silver.tallow_implied_value
                                (period, grade_code, pathway, fuel_type,
                                 fuel_price_per_gal, rin_value_per_gal,
                                 lcfs_value_per_gal, btc_45z_per_gal,
                                 state_credit_per_gal, total_revenue_per_gal,
                                 processing_cost_per_gal, lbs_per_gal,
                                 implied_value_per_lb, market_price_per_lb,
                                 margin_spread_per_lb, pull_flag,
                                 ci_score_used, run_id)
                            VALUES (%s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s, %s,%s)
                        """, (
                            period, iv.grade_code, iv.pathway, iv.fuel_type,
                            iv.fuel_price_per_gal, iv.rin_value_per_gal,
                            iv.lcfs_value_per_gal, iv.btc_45z_per_gal,
                            iv.state_credit_per_gal, iv.total_revenue_per_gal,
                            iv.processing_cost_per_gal, iv.lbs_per_gal,
                            iv.implied_value_per_lb, grade_data['market_price_per_lb'],
                            grade_data['margin_spread_per_lb'], grade_data['pull_flag'],
                            iv.ci_score_used, run_id,
                        ))
                        cur.execute("RELEASE SAVEPOINT sp_tiv")
                    except Exception as e:
                        cur.execute("ROLLBACK TO SAVEPOINT sp_tiv")
                        logger.warning(f"  Tallow IV save error: {e}")

                # Save allocation detail to gold
                eia_total = tallow_result['eia_total_mil_lbs']
                for grade_key in ['ebft', 'ibft']:
                    g = tallow_result[grade_key]
                    try:
                        cur.execute("SAVEPOINT sp_tad")
                        cur.execute("""
                            INSERT INTO gold.tallow_allocation_detail
                                (period, run_id, eia_total_tallow_mil_lbs,
                                 grade_code, allocated_mil_lbs, allocated_pct,
                                 best_pathway, implied_value_per_lb,
                                 market_price_per_lb, margin_spread_per_lb,
                                 allocation_method, constraint_notes)
                            VALUES (%s,%s,%s, %s,%s,%s, %s,%s, %s,%s, %s,%s)
                        """, (
                            period, run_id, eia_total,
                            grade_key.upper(), g['allocated_mil_lbs'], g['allocated_pct'],
                            g['best_pathway'], g['implied_value_per_lb'],
                            g['market_price_per_lb'], g['margin_spread_per_lb'],
                            g['allocation_method'], g['constraint_notes'],
                        ))
                        cur.execute("RELEASE SAVEPOINT sp_tad")
                    except Exception as e:
                        cur.execute("ROLLBACK TO SAVEPOINT sp_tad")
                        logger.warning(f"  Tallow detail save error: {e}")

                conn.commit()
                logger.info(f"  Saved tallow split detail (run_id: {run_id[:8]}...)")

    def run(self, period: date, scenario: str = 'base', save: bool = True) -> list:
        """Run allocation for one month, optionally save."""
        results = self.allocate_month(period, scenario)
        if save and results:
            run_id = self.save_results(period, results, scenario)
            # Save tallow split if we ran it
            if hasattr(self, '_last_tallow_result') and self._last_tallow_result:
                self.save_tallow_split(period, self._last_tallow_result, run_id)
            # Sync to eia_data.xlsm for downstream balance sheet consumption.
            # Best-effort — don't let an Excel sync failure fail the allocation run.
            try:
                self._write_to_eia_data()
            except Exception as e:
                logger.warning(f"Post-run eia_data.xlsm sync failed: {e}")
        return results

    def _write_to_eia_data(self):
        """Push bronze.historical_feedstock_allocation -> eia_data.xlsm.

        Runs the scripts/write_allocation_to_eia_data.py logic inline so the
        xlsm stays in sync after every allocation run. If the eia_data file
        is open in Excel, this will log a warning but not fail the run.
        """
        import subprocess, sys as _sys
        from pathlib import Path as _Path
        script = _Path(__file__).parent.parent.parent.parent / 'scripts' / 'write_allocation_to_eia_data.py'
        if not script.exists():
            logger.warning(f"eia_data sync script not found at {script}")
            return
        result = subprocess.run(
            [_sys.executable, str(script)],
            capture_output=True, text=True, timeout=60,
        )
        if result.returncode != 0:
            logger.warning(f"eia_data sync exited {result.returncode}: "
                           f"{result.stderr[:200]}")
        else:
            logger.info("eia_data.xlsm synced.")

    def run_range(self, start: date, end: date, scenario: str = 'base', save: bool = True):
        """Run allocation for a date range."""
        current = start
        all_results = {}
        while current <= end:
            period = current.replace(day=1)
            results = self.run(period, scenario, save)
            all_results[period] = results

            # Advance to next month
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)

        return all_results


# ═══════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description='Feedstock Allocation Engine')
    parser.add_argument('--period', type=str, help='Single month (YYYY-MM)')
    parser.add_argument('--range', nargs=2, type=str, metavar=('START', 'END'),
                       help='Date range (YYYY-MM YYYY-MM)')
    parser.add_argument('--scenario', type=str, default='base', help='Scenario name')
    parser.add_argument('--no-save', action='store_true', help='Do not save to database')
    args = parser.parse_args()

    allocator = FeedstockAllocator()

    if args.period:
        period = datetime.strptime(args.period, '%Y-%m').date()
        allocator.run(period, args.scenario, save=not args.no_save)
    elif args.range:
        start = datetime.strptime(args.range[0], '%Y-%m').date()
        end = datetime.strptime(args.range[1], '%Y-%m').date()
        allocator.run_range(start, end, args.scenario, save=not args.no_save)
    else:
        # Default: run current month
        today = date.today().replace(day=1)
        allocator.run(today, 'base', save=not args.no_save)


if __name__ == '__main__':
    main()

"""Curve specifications for the curve engine.

Each spec declares WHAT a derived curve is made of; the engine (engine.py) does the reading,
assembly, and transactional write. Methods here are the review surface — Tore approves methods,
not code (brief, Tier B). Every constant that is a modeling choice rather than a fact is tagged
REVIEW POINT with the ruling it needs.

First curve: BRSBO_FOB_PARITY — Brazil soybean oil FOB Paranaguá export parity
(register #19 domestic/export parity + #27 FOB basis, Tier B "BR SBO parity" / curve-module
method 3 IMPORT_PARITY_CHAIN with ZL as parent).

Method (v1):
    headline (USD/t, per delivery window) =
          board          ZL dated-contract settle, cents/lb -> USD/t (x 22.04622)
        + fob_spread     FOB Paranaguá vs CBOT differential   [REVIEW POINT — placeholder 0]
        + basis_residual observed-market residual              [REVIEW POINT — 0 until CEPEA lands]

    * Board leg uses DATED contracts only — the §D.2 aggregator rule forbids anchoring any
      derivation to a continuous/FRONT quote. Contracts with zero reported volume are refused
      (thin guard; OI is NULL in the delayed source, so volume is the §D.1 proxy — noted).
    * FX: none. The curve is quoted USD/t and the parent trades in USD, so no FX term exists in
      this chain. FX terms enter when a local-currency curve (e.g. DCE P from FCPO) is specced.
    * quality: headline DERIVED_PARITY; can_republish=FALSE while the board leg is delayed
      yfinance data [REVIEW POINT — flips TRUE when the official CME ZL collector (register #5)
      replaces it; the engine picks the best-ranked mark automatically, the flag does not].
    * band: NOT stored — migrations 157/158 have no band columns. The brief's "no bandless curve
      leaves gold" rule needs a storage + method ruling before any band is published
      [REVIEW POINT — flagged in the build handoff].
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class FixedTerm:
    """A spec-constant term (spread, duty, levy...) pending or following a calibration ruling."""
    name: str
    value: float
    quality_rank: str
    source: str
    method_note: str


@dataclass(frozen=True)
class ParityChainSpec:
    curve_key: str            # == the headline's silver.price_mark.series_key (tie-out linkage)
    parent_series: str        # board parent in silver.price_mark (dated CONTRACT rows)
    unit: str                 # headline/term unit
    currency: str
    unit_factor: float        # parent value -> curve unit multiplier
    unit_note: str            # how unit_factor was derived (goes into the board term's method_note)
    fixed_terms: tuple = field(default_factory=tuple)
    quality_rank: str = "DERIVED_PARITY"
    can_republish: bool = False
    min_volume: int = 1       # refuse tenors below this reported volume (thin guard)
    description: str = ""


BRSBO_FOB_PARITY = ParityChainSpec(
    curve_key="BRSBO_FOB_PARITY",
    parent_series="ZL",
    unit="USD/t",
    currency="USD",
    # cents/lb -> USD/t: 2204.6226 lb/t / 100 cents. Same 2204.6 lb/MT constant as CLAUDE.md.
    unit_factor=22.046226,
    unit_note="ZL settle cents/lb x 22.046226 = USD/t (2204.6226 lb per metric ton / 100)",
    fixed_terms=(
        FixedTerm(
            name="fob_spread",
            value=0.0,
            quality_rank="PROXY_SPREAD",
            source="rlc_uncalibrated",
            method_note=(
                "FOB Paranaguá vs CBOT ZL differential. PLACEHOLDER 0 — no in-house BR FOB series "
                "exists to calibrate against (checked bronze.feedstock_prices 2026-08-03: no "
                "Brazil/Paranaguá SBO region). REVIEW POINT: Tore to rule the initial spread and "
                "its source (CEPEA collector, register #19, is the intended calibrator)."
            ),
        ),
        FixedTerm(
            name="basis_residual",
            value=0.0,
            quality_rank="PROXY_SPREAD",
            source="rlc_uncalibrated",
            method_note=(
                "Observed-market residual (CEPEA/ESALQ daily indicator vs parity). 0 until the "
                "CEPEA collector lands. REVIEW POINT: residual becomes the RLC-built BR basis "
                "series once observed (register #27)."
            ),
        ),
    ),
    can_republish=False,
    min_volume=1,
    description="Brazil SBO FOB Paranaguá export parity from dated ZL contracts (register #19/#27)",
)


CURVE_SPECS = [BRSBO_FOB_PARITY]

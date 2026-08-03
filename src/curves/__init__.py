"""Curve construction module (Helios price-feeds brief, "Curve construction module").

Turns silver.price_mark marks into derived curves stored as IFV term stacks:
term rows in gold.curve_term + a DERIVED_* headline mark in silver.price_mark
(curve_key == series_key), written in ONE transaction so the deferred
curve_term_tieout trigger validates SUM(terms) == headline at COMMIT.
"""

from src.curves.engine import CurveEngine
from src.curves.specs import CURVE_SPECS

__all__ = ["CurveEngine", "CURVE_SPECS"]

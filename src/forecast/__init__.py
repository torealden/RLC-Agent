"""Forecast layer (ledger 6b; design forecast_layer_design_v1.md).

Home for the mechanical-forecast book (book b): storage helpers, the standing flat-file guards,
and — as they land — the pure (data, assumptions) forecast callables (D5). The LLM scoreboard
(book a) lives in core.forecasts and is deliberately NOT part of this package (D6: the two books
never share code paths any more than they share tables).
"""

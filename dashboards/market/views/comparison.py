"""
Projection Comparison page — placeholder until migration 165 lands (Phase 3).
"""
import streamlit as st


def render() -> None:
    st.subheader('Projection Comparison')
    st.info('Coming in Phase 3: overlay your projections, LLM forecasts, and '
            'USDA WASDE vintages against realized actuals per series. '
            'Requires gold.projection_comparison_long (migration 165).')

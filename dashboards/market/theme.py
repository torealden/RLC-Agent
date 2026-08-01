"""
Shared theme for the RLC Market Dashboard.

Palette lifted from src/dashboard/app.py (house style) plus the RLC green
used in workbooks for Tore's own estimates. The comparison-overlay trio
(RLC_GREEN / llm blue / usda orange) is validated colorblind-safe in both
light and dark modes.
"""

COLORS = {
    'primary': '#1f4e79',
    'secondary': '#2e75b6',
    'accent': '#c55a11',
    'positive': '#548235',
    'negative': '#c00000',
    'neutral': '#7f7f7f',
    'gold': '#f4b942',
    'corn': '#f4b942',
    'soybeans': '#548235',
    'wheat': '#c55a11',
    'soybean_oil': '#2e75b6',
    'soybean_meal': '#8faadc',
    'palm_oil': '#e06c2e',
}

COMMODITY_COLORS = ['#1f4e79', '#2e75b6', '#548235', '#c55a11', '#f4b942',
                    '#7030a0', '#c00000', '#8faadc', '#e06c2e', '#a9d18e']

# Font color Tore uses in the model workbooks for his own estimates.
RLC_GREEN = '#3C7D22'

# Projection-comparison series colors (validated palette).
SOURCE_COLORS = {
    'realized': '#7f7f7f',   # rendered as heavy neutral line; works on light+dark
    'user': RLC_GREEN,
    'llm': '#2e75b6',
    'usda': '#c55a11',
}
SOURCE_LABELS = {
    'realized': 'Realized',
    'user': 'Mine (RLC)',
    'llm': 'LLM',
    'usda': 'USDA',
}

# ── Futures symbol metadata ─────────────────────────────────────────────────
# Order matters: it is the display order of the strip and every picker.
SYMBOLS = {
    # group 'ag'
    'ZC':   {'name': 'Corn',        'group': 'ag',     'unit': '¢/bu',    'decimals': 2},
    'ZS':   {'name': 'Soybeans',    'group': 'ag',     'unit': '¢/bu',    'decimals': 2},
    'ZL':   {'name': 'Soy Oil',     'group': 'ag',     'unit': '¢/lb',    'decimals': 2},
    'ZM':   {'name': 'Soy Meal',    'group': 'ag',     'unit': '$/ton',        'decimals': 2},
    'ZW':   {'name': 'SRW Wheat',   'group': 'ag',     'unit': '¢/bu',    'decimals': 2},
    'KE':   {'name': 'HRW Wheat',   'group': 'ag',     'unit': '¢/bu',    'decimals': 2},
    'ZR':   {'name': 'Rough Rice',  'group': 'ag',     'unit': '$/cwt',        'decimals': 2},
    'DC':   {'name': 'Class III Milk', 'group': 'ag',  'unit': '$/cwt',        'decimals': 2},
    'FCPO': {'name': 'Palm Oil',    'group': 'ag',     'unit': 'MYR/t',        'decimals': 0},
    # group 'energy'
    'CL':   {'name': 'WTI Crude',   'group': 'energy', 'unit': '$/bbl',        'decimals': 2},
    'HO':   {'name': 'ULSD',        'group': 'energy', 'unit': '$/gal',        'decimals': 4},
    'RB':   {'name': 'RBOB',        'group': 'energy', 'unit': '$/gal',        'decimals': 4},
    'NG':   {'name': 'Nat Gas',     'group': 'energy', 'unit': '$/MMBtu',      'decimals': 3},
}


def symbol_name(sym: str) -> str:
    return SYMBOLS.get(sym, {}).get('name', sym)


def fmt_price(sym: str, value) -> str:
    if value is None:
        return '—'
    d = SYMBOLS.get(sym, {}).get('decimals', 2)
    return f'{float(value):,.{d}f}'


def my_label(my: int) -> str:
    """2024 -> '2024/25' (house marketing-year format)."""
    return f'{my}/{(my + 1) % 100:02d}'


def chart_layout(**overrides) -> dict:
    """Common Plotly layout defaults: transparent surfaces, recessive grid,
    horizontal legend. Neutral grays so light and dark themes both work."""
    layout = dict(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        margin=dict(t=50, b=40, l=60, r=30),
        legend=dict(orientation='h', yanchor='bottom', y=1.02, x=0),
        xaxis=dict(gridcolor='rgba(128,128,128,0.10)'),
        yaxis=dict(gridcolor='rgba(128,128,128,0.18)'),
        hovermode='x unified',
    )
    layout.update(overrides)
    return layout

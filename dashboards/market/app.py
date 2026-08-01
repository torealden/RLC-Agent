"""
RLC Market Dashboard
Futures price strip (always on) + projection comparison + series explorer.

Launch: scripts/launch_market_dashboard.bat
    or: streamlit run dashboards/market/app.py --server.port 8510
"""
import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dashboards.market.components.price_strip import render_price_strip  # noqa: E402
from dashboards.market.views import comparison, explorer, markets  # noqa: E402

st.set_page_config(page_title='RLC Market Dashboard', page_icon='📈',
                   layout='wide')

st.sidebar.title('RLC Commodities')
st.sidebar.caption('Market Dashboard')
page = st.sidebar.radio(
    'Navigate', ['Markets', 'Projection Comparison', 'Series Explorer'])
st.sidebar.markdown('---')
st.sidebar.caption('Prices are prior-session settles from '
                   'silver.futures_price (yfinance daily collector), '
                   'not live quotes.')

render_price_strip()

PAGES = {
    'Markets': markets.render,
    'Projection Comparison': comparison.render,
    'Series Explorer': explorer.render,
}
PAGES[page]()

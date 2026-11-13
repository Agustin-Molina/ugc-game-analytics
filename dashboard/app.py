"""Dashboard del dataset UGC Games (Streamlit).

    uv run --group dashboard streamlit run dashboard/app.py

Lee el export de Kaggle (`python -m pipeline.export --out export`) o $UGC_EXPORT_DIR.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st  # noqa: E402

from dashboard import ui  # noqa: E402

st.set_page_config(page_title="UGC Games Tracker", page_icon=":material/sports_esports:",
                   layout="wide")

VIEWS = Path(__file__).parent / "views"
nav = st.navigation([
    st.Page(VIEWS / "overview.py", title="Overview", icon=":material/insights:", default=True),
    st.Page(VIEWS / "niches.py", title="Niches", icon=":material/grid_view:"),
    st.Page(VIEWS / "launches.py", title="Launches", icon=":material/rocket_launch:"),
    st.Page(VIEWS / "game.py", title="Game lookup", icon=":material/search:"),
    st.Page(VIEWS / "clones.py", title="Clones", icon=":material/content_copy:"),
])

with st.sidebar:
    f = ui.get("freshness")
    st.caption(
        f"Roblox: last snapshot {ui.ago(f['roblox_last'])}  \n"
        f"Fortnite: last full day {f['fortnite_last_day']:%Y-%m-%d}  \n"
        f"Tracking since {f['tracking_since']:%Y-%m-%d}")
    st.caption("[Code & methodology](https://github.com/Agustin-Molina/ugc-game-analytics) · "
               "Not affiliated with Roblox or Epic Games.")

nav.run()

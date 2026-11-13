import altair as alt
import streamlit as st

from dashboard import ui

st.title("UGC Games: Roblox & Fortnite Tracker")
st.markdown(
    "Live tracker of user-generated games, collected automatically: Roblox experiences every "
    "hour to every week depending on their traction, and the whole public Fortnite catalog. "
    "Built to find **which niches are open**, **what makes a game take off** and **how to "
    "spot it in its first days**.")

f = ui.get("freshness")
c = st.columns(4)
c[0].metric("Roblox games", f"{f['roblox_games']:,}", help="Tracked experiences")
c[1].metric("Fortnite islands", f"{f['fortnite_islands']:,}", help="Listed public islands")
c[2].metric("Roblox data", ui.ago(f["roblox_last"]), help="Age of the last snapshot")
c[3].metric("Since", f"{f['tracking_since']:%b %d}", help="Start of tracking")

colors = ui.series_colors()
TOOLTIP = ["name", "genre", alt.Tooltip("ccu", title="CCU", format=",.0f")]
left, right = st.columns(2)
with left:
    top = ui.get("top_now", "roblox")
    ui.show(ui.hbar(top, "ccu", "name", "Roblox · most played now (CCU)", colors[0],
                    tooltip=TOOLTIP))
with right:
    top = ui.get("top_now", "fortnite")
    ui.show(ui.hbar(top, "ccu", "name", "Fortnite · most played, last full day (peak CCU)",
                    colors[1], tooltip=TOOLTIP))
st.caption("Fortnite's top spots include Epic's own modes (e.g. Reload); niche analyses "
           "exclude them.")

st.subheader("When people play")
cycle = ui.get("daily_cycle")
ui.show(ui.lines(cycle, "hour", "index", "platform", x_title="hour of day (UTC)",
                 y_title="CCU index (daily mean = 100)", scale=ui.platform_scale(), y_fmt="d",
                 points=True, zero=False))
st.caption("Total concurrent players by hour, indexed to each platform's daily mean. Fortnite "
           "swings harder and peaks in the US afternoon; Roblox's audience is spread over more "
           "time zones.")

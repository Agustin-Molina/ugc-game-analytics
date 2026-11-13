import numbers

import pandas as pd
import streamlit as st

from dashboard import ui

st.title("Game lookup")

params = st.query_params
text = st.text_input("Search by name (or Fortnite island code)",
                     value=params.get("q", ""), placeholder="e.g. brainrot, tycoon, 1v1")
if not text.strip():
    st.info("Type part of a game name to search both platforms.")
    st.stop()
params["q"] = text

results = ui.get("search_games", text)
if results.empty:
    st.warning("No games found.")
    st.stop()
event = st.dataframe(
    results, hide_index=True, width="stretch", on_select="rerun",
    selection_mode="single-row", key="g_results",
    column_order=["platform", "name", "genre", "tier", "peak_ccu"],
    column_config={"platform": st.column_config.TextColumn("Platform"),
                   "name": "Name", "genre": "Genre", "tier": "Tier",
                   "peak_ccu": st.column_config.NumberColumn("Peak CCU", format="localized")})
row = results.iloc[event.selection.rows[0] if event.selection.rows else 0]
platform, gid = row["platform"], row["id"]

info = ui.get("game_info", platform, gid)
st.header(info["name"])
st.caption(f"{ui.PLATFORM_LABEL[platform]} · id `{gid}` · genre **{info['genre']}** · "
           f"tier **{info['tier']}** · creator **{info['creator']}**")


def fmt(v, spec="{:,.0f}"):
    return "—" if v is None or pd.isna(v) else spec.format(v)


# las métricas de lanzamiento no existen si la tracción fue antes del tracking
launch = not info["traction_censored"]
m = st.columns(5)
m[0].metric("Peak CCU (observed)", fmt(info["peak_ccu"]))
m[1].metric("Days to peak", fmt(info["days_to_peak"]) if launch else "—")
m[2].metric("Growth days 0–6", fmt(info["slope_d1_7"], "{:+.2f}") if launch else "—")
m[3].metric("Half-life (days)", fmt(info["half_life_days"], "{:.1f}"))
if platform == "roblox":
    m[4].metric("Like ratio", fmt(info["like_ratio"], "{:.0%}"))
else:
    m[4].metric("D1 retention", fmt(info["retention_d1_mean"], "{:.0%}"))
if not launch:
    st.caption("Had traction before tracking started: its launch was not observed, so there "
               "are no launch metrics.")

series = ui.get("game_series", platform, gid)
if platform == "roblox":
    tabs = {"Concurrent players": "ccu", "Visits": "visits", "Favorites": "favorites"}
else:
    tabs = {"Peak CCU": "ccu", "Unique players": "unique_players", "Plays": "plays",
            "D1 retention": "retention_d1"}
for tab, col in zip(st.tabs(list(tabs)), tabs.values(), strict=True):
    with tab:
        d = series[["t", col]].dropna().assign(series=info["name"][:40])
        ui.show(ui.lines(d, "t", col, "series", x_title=None, y_title=col.replace("_", " "),
                         x_type="temporal", y_fmt=".0%" if col == "retention_d1" else "~s",
                         height=300, points=platform == "fortnite"))

left, right = st.columns(2)
with left:
    st.subheader("Profile")
    if platform == "roblox":
        facts = {
            "Created": info["created_at"], "Last update": info["updated_at"],
            "First seen by the tracker": info["first_seen_at"],
            "First traction": info["first_traction_at"],
            "Roblox genre": f"{info['genre_l1']} / {info['genre_l2']}",
            "Max players": info["max_players"], "Age rating": info["age_rating"],
            "Discovered via": info["discovered_via"],
            "Creator's games": info["creator_total_games"],
            "Creator's previous hits (1M+ visits)": info["creator_prev_hits"],
        }
    else:
        facts = {
            "First seen by the tracker": info["first_seen_at"],
            "First traction": info["first_traction_at"],
            "Tags": ", ".join(info["tags"]) if info["tags"] is not None else None,
            "Licensed IP": info["category"], "Built with": info["created_in"],
            "Creator's other islands": info["creator_other_islands"],
            "Creator's best other island (peak CCU)": info["creator_best_peak_other"],
        }
    facts |= {"Title clone family": info["clone_key"],
              "Family size": info["clone_group_size"], "Rank in family": info["clone_rank"],
              "Bracket tags": ", ".join(info["bracket_tags"])
              if info["bracket_tags"] is not None and len(info["bracket_tags"]) else None}
    def show_value(v) -> str:
        if v is None or (not isinstance(v, str | list) and pd.isna(v)):
            return "—"
        if isinstance(v, pd.Timestamp):
            return v.strftime("%Y-%m-%d %H:%M UTC")
        return f"{v:,.0f}" if isinstance(v, numbers.Number) else str(v)  # incluye numpy

    st.table(pd.DataFrame({"value": [show_value(v) for v in facts.values()]},
                          index=list(facts)))
with right:
    if platform == "roblox":
        st.subheader("Explore charts")
        charts = ui.get("game_charts", gid)
        if charts.empty:
            st.caption("Never seen in an explore chart.")
        else:
            st.dataframe(charts, hide_index=True, width="stretch", column_config={
                "best_rank": "Best rank", "hours_listed": "Snapshots listed",
                "first_seen": st.column_config.DatetimeColumn("First", format="MMM DD HH:mm"),
                "last_seen": st.column_config.DatetimeColumn("Last", format="MMM DD HH:mm")})
    st.subheader("Detected changes")
    events = ui.get("game_events", platform, gid)
    if events.empty:
        st.caption("No changes detected yet.")
    else:
        st.dataframe(events, hide_index=True, width="stretch", column_config={
            "detected_at": st.column_config.DatetimeColumn("Detected", format="MMM DD HH:mm"),
            "event_type": "Change", "old_value": "Before", "new_value": "After"})

if platform == "roblox" and info.get("description"):
    with st.expander("Description"):
        st.text(info["description"])

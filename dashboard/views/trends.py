import altair as alt
import streamlit as st

from dashboard import ui

st.title("Trends")
st.markdown(
    "Formats that grow on one platform and arrive, or not yet, on the other. "
    "**Throw Food For Animals** launched on Roblox on Sep 18, exploded, and dozens of Fortnite "
    "copies followed weeks later. Formats rising on one platform that the other does not have "
    "yet are the window to get there first.\n\n"
    "A theme is a **family** (titles that reduce to the same words: the exact format) or a "
    "**keyword** (one title word: groups a whole wave, like *ragdoll*). Keywords only count "
    "when at least 20% of their games are new, so generic words don't take over.")

level = st.segmented_control("Theme level", ["family", "keyword"], default="family",
                             key="t_level") or "family"
tabs = st.tabs(["Rising on Roblox, missing on Fortnite", "Rising on Fortnite, missing on Roblox",
                "On both", "How long it takes to cross"])

COLS = {
    "theme": st.column_config.TextColumn("Theme"),
    "genre": st.column_config.TextColumn("Genre"),
    "rb_games": st.column_config.NumberColumn("Roblox games"),
    "rb_new_30d": st.column_config.NumberColumn("Roblox new (30 d)"),
    "rb_visits_w1": st.column_config.NumberColumn("Roblox visits / week", format="compact"),
    "rb_top_game": st.column_config.TextColumn("Top Roblox game"),
    "fn_games": st.column_config.NumberColumn("Fortnite islands"),
    "fn_new_30d": st.column_config.NumberColumn("Fortnite new (30 d)"),
    "fn_players_w1": st.column_config.NumberColumn("Fortnite players / week", format="compact"),
    "fn_top_island": st.column_config.TextColumn("Top Fortnite island"),
    "first_platform": st.column_config.TextColumn("First on"),
    "lag_days": st.column_config.NumberColumn("Lag (days)"),
    "rb_first_at": st.column_config.DatetimeColumn("First on Roblox", format="YYYY-MM-DD"),
    "fn_first_at": st.column_config.DatetimeColumn("First on Fortnite", format="YYYY-MM-DD"),
}


def table(df, cols):
    if df.empty:
        st.info("Nothing here yet.")
        return
    st.caption(f"{len(df):,} themes")
    st.dataframe(df, hide_index=True, width="stretch", column_order=cols,
                 column_config=COLS)


with tabs[0]:
    st.markdown("Growing on Roblox (≥ 3 new games in 30 days and ≥ 1M weekly visits) with "
                "≤ 2 islands and no real demand on Fortnite.")
    table(ui.get("trends", "port_to_fortnite", level),
          ["theme", "genre", "rb_games", "rb_new_30d", "rb_visits_w1", "rb_top_game",
           "fn_games", "fn_top_island"])
with tabs[1]:
    st.markdown(
        "Growing on Fortnite (≥ 3 new islands in 30 days and ≥ 5,000 weekly players) with "
        "≤ 2 games and no real demand on Roblox. Fortnite build/combat formats (1v1, zone "
        "wars, pillars…) are left out. Weaker than the other direction: titles are matched "
        "word by word, and Roblox calls some formats differently (*obby* vs *parkour*).")
    table(ui.get("trends", "port_to_roblox", level),
          ["theme", "genre", "fn_games", "fn_new_30d", "fn_players_w1", "fn_top_island",
           "rb_games", "rb_top_game"])
with tabs[2]:
    st.markdown("On both platforms with new copies in the last 30 days.")
    table(ui.get("trends", "crossed", level),
          ["theme", "genre", "first_platform", "lag_days", "rb_games", "rb_new_30d",
           "rb_visits_w1", "fn_games", "fn_new_30d", "fn_players_w1", "rb_top_game",
           "fn_top_island"])
with tabs[3]:
    st.markdown(
        "Families that crossed with both dates known. Fortnite does not publish creation "
        "dates, so only formats that reached Fortnite after tracking started (Nov 10, 2026) "
        "have a lag; this list grows every week.")
    lags = ui.get("trend_lags")
    table(lags, ["theme", "first_platform", "lag_days", "rb_first_at", "fn_first_at",
                 "rb_top_game", "fn_top_island"])
    if not lags.empty:
        colors = ui.series_colors()
        ui.show(alt.Chart(lags, title="Days for a format to cross platforms")
                .mark_bar(color=colors[0], cornerRadiusEnd=3)
                .encode(x=alt.X("lag_days:Q", bin=alt.Bin(step=7), title="lag (days, 1-week bins)"),
                        y=alt.Y("count():Q", title="formats"),
                        tooltip=[alt.Tooltip("count():Q", title="formats")])
                .properties(height=220))

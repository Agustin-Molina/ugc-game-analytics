import altair as alt
import streamlit as st

from dashboard import ui

st.title("Niches")
st.markdown(
    "Demand vs. competition per unified genre (25 genres shared by both platforms). Each "
    "platform measures demand differently, so scores are **not comparable across platforms**. "
    "Week-over-week growth and the opportunity score need two full weeks of data; until then "
    "they are empty.\n\n"
    "A single breakout game can carry a whole niche for a week, so the **without top game** "
    "columns leave out the game that gained the most, and **growing / falling** count how many "
    "games moved each way. If a niche still grows without its top game, demand is niche-wide.")

GROWTH_HELP = ("Week-over-week growth without the game that gained the most: if it stays "
               "positive, the niche grows beyond a single hit.")

roblox_tab, fortnite_tab = st.tabs(["Roblox", "Fortnite"])
colors = ui.series_colors()

with roblox_tab:
    df = ui.get("niches", "roblox")
    min_active = st.slider("Minimum active games", 0, 100, 20, key="r_min")
    df = df[df["games_active"] >= min_active]
    sort = st.selectbox("Sort by", {
        "new_games_visits_share": "Share of visits taken by new games",
        "opportunity_score_ex_top1": "Opportunity without the top game",
        "median_game_visits": "Visits of the typical game",
        "opportunity_score": "Opportunity score",
        "visits_w1": "Total weekly visits",
    }.items(), format_func=lambda kv: kv[1], key="r_sort")[0]
    df = df.sort_values(sort, ascending=False, na_position="last")
    st.dataframe(
        df, hide_index=True, width="stretch",
        column_order=["genre_unified", "games_active", "visits_w1", "median_game_visits",
                      "top10_share", "new_games_30d", "new_games_visits_share",
                      "visits_growth", "visits_growth_ex_top1", "games_growing",
                      "games_falling", "opportunity_score", "opportunity_score_ex_top1"],
        column_config={
            "genre_unified": st.column_config.TextColumn("Genre"),
            "games_active": st.column_config.NumberColumn("Active games"),
            "visits_w1": st.column_config.NumberColumn("Visits / week", format="compact"),
            "median_game_visits": st.column_config.NumberColumn(
                "Typical game (median visits)", format="compact"),
            "top10_share": st.column_config.ProgressColumn(
                "Top 10 share", format="percent", min_value=0, max_value=1),
            "new_games_30d": st.column_config.NumberColumn("New games (30 d)"),
            "new_games_visits_share": st.column_config.ProgressColumn(
                "Visits to new games", format="percent", min_value=0, max_value=1,
                help="Share of the niche's weekly visits taken by games created in the last "
                     "30 days: how much room the niche leaves for a newcomer."),
            "visits_growth": st.column_config.NumberColumn("Growth w/w", format="percent"),
            "visits_growth_ex_top1": st.column_config.NumberColumn(
                "Growth w/o top game", format="percent", help=GROWTH_HELP),
            "games_growing": st.column_config.NumberColumn(
                "Growing", help="Games whose weekly visits went up (≥ 1,000 a week)."),
            "games_falling": st.column_config.NumberColumn(
                "Falling", help="Games whose weekly visits went down (≥ 1,000 a week)."),
            "opportunity_score": st.column_config.NumberColumn(
                "Opportunity", format="compact",
                help="Weekly visits gained week over week / active games."),
            "opportunity_score_ex_top1": st.column_config.NumberColumn(
                "Opportunity w/o top game", format="compact"),
        })
    chart_df = df.sort_values("new_games_visits_share", ascending=False)
    ui.show(ui.hbar(chart_df, "new_games_visits_share", "genre_unified",
                    "Room for newcomers: share of weekly visits taken by games < 30 days old",
                    colors[0], fmt=".0%",
                    tooltip=[alt.Tooltip("genre_unified", title="genre"),
                             alt.Tooltip("new_games_visits_share", title="visits to new games",
                                         format=".0%"),
                             alt.Tooltip("new_games_30d", title="new games"),
                             alt.Tooltip("median_game_visits", title="typical game visits",
                                         format=",.0f")]))
    st.caption(
        "Demand = visits gained in the week, interpolated between snapshots. Only games seen "
        "in Roblox charts or keyword searches are tracked, so small competitors are "
        "undercounted: compare niches with each other, not as a census.")

with fortnite_tab:
    df = ui.get("niches", "fortnite")
    min_active = st.slider("Minimum active islands", 0, 500, 50, key="f_min")
    df = df[df["islands_active"] >= min_active]
    sort = st.selectbox("Sort by", {
        "opportunity_score_ex_top1": "Opportunity without the top island",
        "median_island_demand": "Players of the typical island",
        "new_islands_demand_share": "Share of players taken by new islands",
        "opportunity_score": "Opportunity score",
        "demand_w1": "Total weekly players",
        "top10_share": "Concentration in the top 10",
    }.items(), format_func=lambda kv: kv[1], key="f_sort")[0]
    df = df.sort_values(sort, ascending=sort == "top10_share", na_position="last")
    st.dataframe(
        df, hide_index=True, width="stretch",
        column_order=["genre_unified", "islands_active", "demand_w1", "median_island_demand",
                      "top10_share", "new_islands_demand_share", "demand_growth",
                      "demand_growth_ex_top1", "islands_growing", "islands_falling",
                      "opportunity_score", "opportunity_score_ex_top1"],
        column_config={
            "genre_unified": st.column_config.TextColumn("Genre"),
            "islands_active": st.column_config.NumberColumn("Active islands"),
            "demand_w1": st.column_config.NumberColumn("Players / week", format="compact"),
            "median_island_demand": st.column_config.NumberColumn(
                "Typical island (median players)", format="%.0f"),
            "demand_per_active_island": st.column_config.NumberColumn(
                "Players per active island", format="compact"),
            "top10_share": st.column_config.ProgressColumn(
                "Top 10 share", format="percent", min_value=0, max_value=1),
            "demand_growth": st.column_config.NumberColumn("Growth w/w", format="percent"),
            "new_islands_demand_share": st.column_config.ProgressColumn(
                "Players to new islands", format="percent", min_value=0, max_value=1,
                help="Share of the niche's weekly players taken by islands that appeared in "
                     "the last 30 days: how much room it leaves for a newcomer."),
            "demand_growth_ex_top1": st.column_config.NumberColumn(
                "Growth w/o top island", format="percent", help=GROWTH_HELP),
            "islands_growing": st.column_config.NumberColumn(
                "Growing", help="Islands whose weekly players went up (≥ 100 a week)."),
            "islands_falling": st.column_config.NumberColumn(
                "Falling", help="Islands whose weekly players went down (≥ 100 a week)."),
            "opportunity_score": st.column_config.NumberColumn(
                "Opportunity", format="%.1f",
                help="Player-days gained week over week / active islands."),
            "opportunity_score_ex_top1": st.column_config.NumberColumn(
                "Opportunity w/o top island", format="%.1f"),
        })
    chart_df = df.sort_values("median_island_demand", ascending=False)
    ui.show(ui.hbar(chart_df, "median_island_demand", "genre_unified",
                    "Weekly unique players of the typical active island", colors[1],
                    tooltip=[alt.Tooltip("genre_unified", title="genre"),
                             alt.Tooltip("median_island_demand", title="median players"),
                             alt.Tooltip("islands_active", title="active islands"),
                             alt.Tooltip("top10_share", title="top 10 share", format=".0%")]))
    st.caption(
        "Demand = unique players summed over each island's last covered week; a day without "
        "data counts as 0 (the API reports nothing under 5 players). Epic's own islands are "
        "excluded.")

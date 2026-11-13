import altair as alt
import pandas as pd
import streamlit as st

from dashboard import ui

st.title("Launches")
st.markdown(
    "Games observed **from their first traction** (first time with ≥ 100 concurrent players). "
    "Games that already had traction when tracking started are excluded: their launch was "
    "not observed. Select rows to compare their curves.")

platform = st.segmented_control("Platform", ["roblox", "fortnite"], default="roblox",
                                format_func=ui.PLATFORM_LABEL.get, key="l_platform") or "roblox"
df = ui.get("launches", platform)

c1, c2, c3 = st.columns([2, 1, 1])
genres = c1.multiselect("Genres", sorted(df["genre"].dropna().unique()), key="l_genres")
min_peak = c2.number_input("Minimum peak CCU", 0, value=100, step=100, key="l_peak")
max_age = c3.number_input("First traction within (days)", 1, value=60, key="l_age")
if genres:
    df = df[df["genre"].isin(genres)]
# relativo a la tracción más reciente del dataset (el export puede tener días)
recent = df["first_traction_at"] >= df["first_traction_at"].max() - pd.Timedelta(days=max_age)
df = df[(df["peak_ccu"] >= min_peak) & recent]
st.caption(f"{len(df):,} launches")

cols = ["name", "genre", "creator", "first_traction_at", "obs_days", "peak_ccu",
        "days_to_peak", "slope_d1_7", "last_day_peak"]
if platform == "fortnite":
    cols.append("retention_d1_mean")
event = st.dataframe(
    df, hide_index=True, width="stretch", column_order=cols,
    on_select="rerun", selection_mode="multi-row", key=f"l_table_{platform}",
    column_config={
        "name": "Name", "genre": "Genre", "creator": "Creator",
        "first_traction_at": st.column_config.DatetimeColumn("First traction",
                                                             format="YYYY-MM-DD HH:mm"),
        "obs_days": st.column_config.NumberColumn("Days observed"),
        "peak_ccu": st.column_config.NumberColumn("Peak CCU", format="localized"),
        "days_to_peak": st.column_config.NumberColumn("Days to peak"),
        "slope_d1_7": st.column_config.NumberColumn(
            "Growth days 0–6", format="%.2f",
            help="Slope of log(1 + daily peak CCU) over the first 7 days: +0.69 ≈ doubling "
                 "every day. Needs 3+ days."),
        "last_day_peak": st.column_config.NumberColumn("Last day peak", format="localized"),
        "retention_d1_mean": st.column_config.NumberColumn("D1 retention", format="percent"),
    })

rows = event.selection.rows if event and event.selection else []
picked = df.iloc[rows] if rows else df.head(5)
picked = picked.head(8)  # 8 colores categóricos como máximo
if picked.empty:
    st.info("No launches match the filters.")
    st.stop()

curves = ui.get("launch_curves", platform, tuple(picked["id"]))
# etiqueta única por juego (puede haber nombres repetidos) y color fijo por orden de selección
labels = {i: f"{n[:40]} · {i[-4:]}" for i, n in zip(picked["id"], picked["name"], strict=True)}
curves["game"] = curves["id"].map(labels)
order = [labels[i] for i in picked["id"]]
log_y = st.toggle("Log scale", value=True, key="l_log")
st.subheader("Curves since first traction" + ("" if rows else " · top 5 by peak"))
ui.show(ui.lines(
    curves, "days", "ccu", "game", x_title="days since first traction",
    y_title="concurrent players" + (" (hourly)" if platform == "roblox" else " (daily peak)"),
    scale=alt.Scale(domain=order, range=ui.series_colors()[:len(order)]), log_y=log_y,
    height=420, points=platform == "fortnite"))

"""Pegamento de Streamlit: conexión y consultas cacheadas, paleta y gráficos Altair."""

from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from dashboard import data

# Paleta categórica validada (orden fijo, nunca ciclado), con pasos propios para modo oscuro.
_LIGHT = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
_DARK = ["#3987e5", "#d95926", "#199e70", "#c98500", "#d55181", "#008300", "#9085e9", "#e66767"]
PLATFORM_LABEL = {"roblox": "Roblox", "fortnite": "Fortnite"}


def series_colors() -> list[str]:
    try:
        dark = st.context.theme.type == "dark"
    except Exception:  # versiones o contextos sin info de tema
        dark = False
    return _DARK if dark else _LIGHT


def platform_scale() -> alt.Scale:
    c = series_colors()
    return alt.Scale(domain=["Roblox", "Fortnite"], range=[c[0], c[1]])


# ------------------------------------------------------------------ datos

@st.cache_resource(show_spinner=False)
def _con(path: str):
    return data.open_db(path)


def data_dir() -> str:
    path = data.find_data_dir()
    if path is None:
        st.error(
            "Dataset not found. Export it first:\n\n"
            "```\nuv run python -m pipeline.export --out export --owner local "
            "--data-dir ../ugc-game-analytics-data\n```\n"
            "or point `UGC_EXPORT_DIR` to a folder with the Kaggle files.")
        st.stop()
    return str(path)


@st.cache_data(ttl=900, show_spinner=False)
def _get(path: str, fn: str, *args):
    return getattr(data, fn)(_con(path), *args)


def get(fn: str, *args):
    """dashboard.data.<fn>(con, *args) con cache por carpeta de datos (args hasheables)."""
    return _get(data_dir(), fn, *args)


# ------------------------------------------------------------------ gráficos

def hbar(df: pd.DataFrame, value: str, label: str, title: str, color: str,
         fmt: str = ",.0f", tooltip: list | None = None, height: int | None = None):
    """Barras horizontales ordenadas, con tooltip por barra."""
    tooltip = tooltip or [alt.Tooltip(label), alt.Tooltip(value, format=fmt)]
    return (alt.Chart(df, title=title)
            .mark_bar(color=color, cornerRadiusEnd=3, height={"band": 0.7})
            .encode(x=alt.X(value, title=None, axis=alt.Axis(format=fmt.replace(",.0f", "~s"))),
                    y=alt.Y(label, sort="-x", title=None,
                            axis=alt.Axis(labelLimit=260, labelOverlap=False)),
                    tooltip=tooltip)
            .properties(height=height or max(180, 28 * len(df))))


def lines(df: pd.DataFrame, x: str, y: str, series: str, *, x_title: str, y_title: str,
          scale: alt.Scale | None = None, log_y: bool = False, y_fmt: str = "~s",
          x_type: str = "quantitative", height: int = 340, extra_tooltip: list | None = None,
          points: bool = False, zero: bool = True):
    """Líneas con capa de hover: regla vertical + punto + tooltip en el x más cercano.

    Rectas entre observaciones (sin suavizar: no inventar forma entre datos); `points` marca
    cada observación, útil para series diarias con pocos puntos.
    """
    scale = scale or alt.Scale(range=series_colors())
    base = alt.Chart(df).encode(
        x=alt.X(x, type=x_type, title=x_title),
        y=alt.Y(y, title=y_title,
                scale=alt.Scale(type="log") if log_y else alt.Scale(zero=zero),
                axis=alt.Axis(format=y_fmt)),
        color=alt.Color(series, scale=scale, legend=alt.Legend(title=None, orient="top")),
    )
    nearest = alt.selection_point(nearest=True, on="pointerover", fields=[x], empty=False)
    x_tip = (alt.Tooltip(x, type=x_type, format="%Y-%m-%d %H:%M") if x_type == "temporal"
             else alt.Tooltip(x, type=x_type, format=",.2f"))
    tooltip = [alt.Tooltip(series), x_tip,
               alt.Tooltip(y, format=".1%" if y_fmt.endswith("%") else ",.0f"),
               *(extra_tooltip or [])]
    line = base.mark_line(strokeWidth=2, point=alt.OverlayMarkDef(size=28) if points else False)
    hit = base.mark_point(size=120, opacity=0).add_params(nearest)
    dots = base.mark_point(size=70, filled=True, strokeWidth=2, stroke="white").encode(
        opacity=alt.when(nearest).then(alt.value(1)).otherwise(alt.value(0)), tooltip=tooltip)
    rule = alt.Chart(df).mark_rule(color="#8c8b85", strokeWidth=1).encode(
        x=alt.X(x, type=x_type)).transform_filter(nearest)
    return alt.layer(line, hit, dots, rule).properties(height=height)


def show(chart):
    st.altair_chart(chart, width="stretch")


def ago(ts) -> str:
    if ts is None or pd.isna(ts):
        return "—"
    t = pd.Timestamp(ts)
    t = t.tz_localize("UTC") if t.tzinfo is None else t
    h = (pd.Timestamp.now(tz="UTC") - t).total_seconds() / 3600
    return f"{h:.0f} h ago" if h < 48 else f"{h / 24:.0f} days ago"

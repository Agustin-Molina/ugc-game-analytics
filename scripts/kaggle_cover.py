"""Genera la portada del dataset de Kaggle (config/kaggle_cover.png, 560×280).

El CLI de Kaggle recorta la portada en (0, 0, 560, 280) y la miniatura en el cuadrado
central, así que la imagen tiene que medir exactamente 560×280 y lo importante va al medio.
De fondo, las curvas horarias de los juegos más jugados de la última semana (normalizadas).

Uso (matplotlib no es dependencia del proyecto):
    uv run --with matplotlib python scripts/kaggle_cover.py --data-dir ../ugc-game-analytics-data
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from pipeline.db import connect  # noqa: E402

SURFACE = "#1a1a19"
INK = "#f4f3ef"
MUTED = "#b5b4ae"
ROBLOX = "#3987e5"     # categórica 1 (oscuro)
FORTNITE = "#d95926"   # categórica 2 (oscuro)

ROBLOX_SQL = """
with top as (
    select universe_id from roblox_snapshots
    where snapshot_at >= now() - interval 7 day
    group by 1 order by max(playing) desc limit 12)
select universe_id as id, date_trunc('hour', snapshot_at) as t, max(playing) as v
from roblox_snapshots join top using (universe_id)
where snapshot_at >= now() - interval 7 day
group by 1, 2 order by 1, 2
"""

FORTNITE_SQL = """
with s as (
    select island_code as code, bucket_start as t, peak_ccu as v from fortnite_snapshots_latest
    where interval = 'hour' and bucket_start >= now() - interval 7 day and peak_ccu is not null),
top as (select code from s group by 1 order by max(v) desc limit 12)
select code as id, t, v from s join top using (code) order by 1, 2
"""


def curves(con, sql: str) -> list[tuple[list, list]]:
    df = con.sql(sql).pl()
    out = []
    for _, g in df.group_by("id"):
        peak = g["v"].max()
        if peak and len(g) > 24:
            out.append((g["t"].to_list(), [v / peak for v in g["v"].to_list()]))
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--data-dir", default=None)
    parser.add_argument("--out", default=str(ROOT / "config" / "kaggle_cover.png"))
    args = parser.parse_args(argv)
    con = connect(args.data_dir)

    fig = plt.figure(figsize=(5.6, 2.8), dpi=100, facecolor=SURFACE)
    # Dos franjas: Roblox arriba, Fortnite abajo, con el título en el medio.
    for color, sql, (bottom, height) in [(ROBLOX, ROBLOX_SQL, (0.52, 0.46)),
                                         (FORTNITE, FORTNITE_SQL, (0.02, 0.46))]:
        ax = fig.add_axes((0, bottom, 1, height))
        ax.set_facecolor(SURFACE)
        ax.set_axis_off()
        for t, v in curves(con, sql):
            ax.plot(t, v, color=color, lw=1.0, alpha=0.38, solid_capstyle="round")
        ax.margins(x=0, y=0.04)

    band = fig.add_axes((0.18, 0.30, 0.64, 0.40))
    band.set_axis_off()
    band.add_patch(plt.Rectangle((0, 0), 1, 1, color=SURFACE, alpha=0.88,
                                 transform=band.transAxes))
    fig.text(0.5, 0.555, "UGC GAMES", ha="center", va="center", color=INK,
             fontsize=27, fontweight="bold")
    fig.text(0.5, 0.415, "Roblox  ×  Fortnite tracker", ha="center", va="center",
             color=MUTED, fontsize=10.5)
    fig.text(0.5, 0.335, "hourly & daily player metrics", ha="center", va="center",
             color=MUTED, fontsize=8)
    # Identidad no solo por color: rótulos en las franjas.
    fig.text(0.03, 0.93, "Roblox · top games CCU", color=INK, fontsize=7, alpha=0.9)
    fig.text(0.03, 0.04, "Fortnite · top islands CCU", color=INK, fontsize=7, alpha=0.9)

    fig.savefig(args.out, dpi=100, facecolor=SURFACE)
    print(f"portada: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

import math
from datetime import UTC, date, datetime, timedelta

import polars as pl
import pytest

from collectors.common import TS, write_dimension_changes, write_partition
from pipeline.db import connect

D0 = datetime(2026, 10, 1, tzinfo=UTC)
# crece 20 %/día hasta el día 5, después cae; el día 8 ya está por debajo de la mitad
PEAKS = [100, 120, 144, 173, 207, 249, 200, 150, 124, 100, 90, 80, 70, 60]


def ols_slope(ys: list[float]) -> float:
    xs = list(range(len(ys)))
    mx, my = sum(xs) / len(xs), sum(ys) / len(ys)
    return (sum((x - mx) * (y - my) for x, y in zip(xs, ys, strict=True))
            / sum((x - mx) ** 2 for x in xs))


def rows(con, sql):
    return {r[next(iter(r))]: r for r in con.sql(sql).pl().to_dicts()}


@pytest.fixture
def fortnite(tmp_path):
    games = pl.DataFrame({
        "island_code": ["new", "old", "flat"],
        "first_traction_at": [D0, D0, None],
        # "old" estaba en el catálogo inicial; "new" apareció después
        "first_seen_at": [D0 - timedelta(days=1), D0 - timedelta(days=30),
                          D0 - timedelta(days=30)],
    }, schema={"island_code": pl.Utf8, "first_traction_at": TS, "first_seen_at": TS})
    write_dimension_changes(games.clear(), games, "island_code", tmp_path, "fortnite", D0, "g")
    snaps = []
    for t, p in enumerate(PEAKS):
        snaps.append(("new", D0 + timedelta(days=t), False, p, 10 * p, p // 2, 0.4))
    snaps.append(("new", D0 + timedelta(days=14), True, 5, 50, 1, None))   # bucket abierto
    for t in range(3):
        snaps.append(("old", D0 + timedelta(days=t), False, 500 - t, 5000, 10, 0.2))
        snaps.append(("flat", D0 + timedelta(days=t), False, 3, 30, 0, None))
    df = pl.DataFrame(
        [{"island_code": c, "interval": "day", "bucket_start": b, "snapshot_at": D0
          + timedelta(days=20), "is_partial": part, "peak_ccu": p, "unique_players": u,
          "favorites": f, "recommendations": 1, "retention_d1": r, "retention_d7": None,
          "plays": u, "avg_minutes_per_player": 12.0, "minutes_played": 1}
         for c, b, part, p, u, f, r in snaps],
        schema_overrides={"bucket_start": TS, "snapshot_at": TS, "retention_d1": pl.Float64,
                          "retention_d7": pl.Float64})
    write_partition(df, tmp_path, "fortnite", "snapshots", D0 + timedelta(days=20), "s")
    return connect(tmp_path)


def test_fortnite_curve_metrics(fortnite):
    m = rows(fortnite, "select * from fortnite_game_metrics")
    n = m["new"]
    assert n["obs_days"] == 14  # el bucket abierto no cuenta
    assert n["peak_ccu"] == 249 and n["peak_day"] == date(2026, 10, 6)
    assert n["anchor_day"] == date(2026, 10, 1) and n["traction_censored"] is False
    assert n["days_to_peak"] == 5
    assert n["half_life_days"] == 3          # 249 → 124 el día 8
    assert n["days_since_peak"] == 8
    assert n["slope_d1_7"] == pytest.approx(ols_slope([math.log1p(p) for p in PEAKS[:7]]))
    assert n["slope_d1_14"] == pytest.approx(ols_slope([math.log1p(p) for p in PEAKS]))
    assert n["sustained_ratio"] is None      # sin datos de los días 30–60
    assert n["peak_over_median"] == pytest.approx(249 / 122)
    assert n["favorites_per_player"] == pytest.approx(
        sum(p // 2 for p in PEAKS) / sum(10 * p for p in PEAKS))
    assert n["retention_d1_mean"] == pytest.approx(0.4)

    o = m["old"]
    assert o["traction_censored"] is True    # ya tenía tracción el primer día que lo vimos
    assert o["half_life_days"] is None and o["slope_d1_7"] == pytest.approx(
        ols_slope([math.log1p(p) for p in (500, 499, 498)]))
    f = m["flat"]
    assert f["anchor_day"] is None and f["days_to_peak"] is None and f["slope_d1_7"] is None
    assert f["peak_day"] == date(2026, 10, 1)  # empate: el primer día con el máximo


def test_fortnite_daily_view(fortnite):
    d = fortnite.sql("select count(*), min(day), max(day) from fortnite_daily "
                     "where island_code = 'new'").fetchall()[0]
    assert d == (14, date(2026, 10, 1), date(2026, 10, 14))


def test_roblox_metrics(tmp_path):
    # fechas recientes: visits_per_day_7d mira los últimos 7 días hasta hoy
    R0 = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=4)
    games = pl.DataFrame({
        "universe_id": [1, 2],
        "first_traction_at": [R0 + timedelta(hours=2), R0 + timedelta(days=1, hours=1)],
        "created_at": [R0, datetime(2020, 1, 1, tzinfo=UTC)],
    }, schema={"universe_id": pl.Int64, "first_traction_at": TS, "created_at": TS})
    write_dimension_changes(games.clear(), games, "universe_id", tmp_path, "roblox", R0, "g")
    now = datetime.now(UTC)
    snaps = [  # (universe, ts, playing, visits, favorites, up, down)
        (1, R0 + timedelta(hours=2), 150, 1000, 10, 8, 2),
        (1, R0 + timedelta(hours=9), 300, 2000, 20, 8, 2),
        (1, R0 + timedelta(days=1, hours=3), 600, 4000, 40, 9, 1),
        (1, R0 + timedelta(days=2, hours=3), 200, 0, 50, 9, 1),     # visitas 0: glitch
        (1, R0 + timedelta(days=3, hours=3), 100, 7000, 70, 18, 2),
        (1, now, 99999, 9000, 90, 18, 2),                            # hoy: día abierto
        (2, R0 + timedelta(days=1, hours=1), 400, 5000, 5, 1, 1),
    ]
    df = pl.DataFrame(
        [{"snapshot_at": t, "universe_id": u, "playing": p, "visits": v, "favorites": f,
          "up_votes": up, "down_votes": dn} for u, t, p, v, f, up, dn in snaps],
        schema_overrides={"snapshot_at": TS})
    for i, part in enumerate(df.partition_by("snapshot_at")):
        write_partition(part, tmp_path, "roblox", "snapshots", part["snapshot_at"][0],
                        f"s{i}")
    con = connect(tmp_path)

    daily = con.sql("select day, peak_ccu, n_snapshots, is_partial from roblox_daily "
                    "where universe_id = 1 order by day").fetchall()
    assert daily[0] == (R0.date(), 300, 2, False)
    assert daily[-1][3] is True

    m = rows(con, "select * from roblox_game_metrics")
    g = m[1]
    assert g["peak_ccu"] == 600 and g["days_to_peak"] == 1   # el 99999 de hoy no cuenta
    assert g["traction_censored"] is False and g["obs_days"] == 4
    assert g["half_life_days"] == 1   # 600 → 200 al día siguiente
    assert g["slope_d1_7"] == pytest.approx(
        ols_slope([math.log1p(p) for p in (300, 600, 200, 100)]))
    # último estado (incluye hoy): 90 favoritos / 9000 visitas, 18 / 20 likes
    assert g["favorites_per_visit"] == pytest.approx(0.01)
    assert g["like_ratio"] == pytest.approx(0.9) and g["visits_last"] == 9000
    assert g["visits_per_day_7d"] > 0
    assert m[2]["traction_censored"] is True  # juego viejo con tracción al verlo
    assert m[2]["visits_per_day_7d"] is None  # un solo día

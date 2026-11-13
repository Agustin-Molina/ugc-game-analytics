"""Dashboard: funciones de datos y humo de cada página sobre un export mínimo.

El export de prueba tiene solo las columnas que usa el dashboard: si el export de Kaggle
cambia de forma, estos tests lo muestran.
"""

import duckdb
import pytest

from dashboard import data

st_testing = pytest.importorskip("streamlit.testing.v1")

T = "timestamptz '2026-11-12 12:00:00+00'"
TABLES = {
    "roblox_games": f"""
        select * from (values
          (1, 'Steal a Brainrot', 'tycoon', 'hot', 'Studio', 'Group', {T} - interval 3 day,
           {T} - interval 10 day, 'Simulation', 'Tycoon', {T}, {T} - interval 4 day, 20,
           '9+', 'chart', 'steal brainrot', ['UPD'], true, 1, 3, 1, 5000000,
           'brainrot steal', 2, 1, 'desc'),
          (2, 'Old Obby', 'obby_parkour', 'warm', 'Someone', 'User', null,
           timestamptz '2020-01-01 00:00:00+00', 'Obby & Platformer', '', {T},
           {T} - interval 4 day, 10, '9+', 'search', 'old obby', [], false, 0, 1, 0, null,
           null, 1, null, null))
        t(universe_id, name, genre_unified, tier, creator_name, creator_type,
          first_traction_at, created_at, genre_l1, genre_l2, updated_at, first_seen_at,
          max_players, age_rating, discovered_via, title_clean, bracket_tags, has_update_tag,
          n_emojis, creator_total_games, creator_prev_hits, creator_prev_max_visits,
          clone_key, clone_group_size, clone_rank, description)""",
    "roblox_snapshots": f"""
        select universe_id, {T} - to_hours(h) as snapshot_at, 100 * universe_id + h as playing,
               1000 + h as visits, 10 as favorites
        from range(0, 48) r(h), (values (1), (2)) u(universe_id)""",
    "roblox_game_metrics": """
        select * from (values
          (1, 500, date '2026-11-11', 1, 0.5, null, false, 0.9, 0.01, 2000, date '2026-11-09', 3),
          (2, 247, date '2026-11-10', null, null, null, true, 0.8, 0.02, 1000, null, 2))
        t(universe_id, peak_ccu, peak_day, days_to_peak, slope_d1_7, half_life_days,
          traction_censored, like_ratio, favorites_per_visit, visits_last, anchor_day,
          obs_days)""",
    "roblox_daily": """
        select * from (values (1, date '2026-11-11', 500), (2, date '2026-11-11', 247))
        t(universe_id, day, peak_ccu)""",
    "roblox_chart_positions": f"""
        select 'Up-and-Coming' as sort_name, 3 as rank, {T} as snapshot_at, 1 as universe_id""",
    "roblox_events": f"""
        select 1 as universe_id, {T} as detected_at, 'name_change' as event_type,
               'Old' as old_value, 'Steal a Brainrot' as new_value""",
    "roblox_niche_opportunity": """
        select 'tycoon' as genre_unified, 30 as games_tracked, 25 as games_active,
               1e6 as visits_w1, null::double as visits_growth, 4e4 as visits_per_active_game,
               2e4 as median_game_visits, 0.6 as top10_share, 3 as new_games_30d,
               0.2 as new_games_visits_share, null::double as opportunity_score""",
    "fortnite_islands": f"""
        select * from (values
          ('1111-2222-3333', 'Brainrot Steal', 'tycoon', 'hot', 'maker', true,
           {T} - interval 4 day, ['tycoon'], null, 'UEFN', {T} - interval 5 day,
           'brainrot steal', [], false, 0, 2, 80, 'brainrot steal', 2, 2))
        t(island_code, title, genre_unified, tier, creator_code, is_listed,
          first_traction_at, tags, category, created_in, first_seen_at, title_clean,
          bracket_tags, has_update_tag, n_emojis, creator_other_islands,
          creator_best_peak_other, clone_key, clone_group_size, clone_rank)""",
    "fortnite_daily": """
        select '1111-2222-3333' as island_code, date '2026-11-08' + d::int as day,
               100 + d as peak_ccu, 500 as unique_players, 900 as plays,
               0.2 as retention_d1, 30.0 as avg_minutes_per_player
        from range(0, 4) r(d)""",
    "fortnite_snapshots": f"""
        select {T} - to_hours(h) as bucket_start, 'hour' as interval, 50 + h as peak_ccu
        from range(0, 30) r(h)""",
    "fortnite_island_metrics": """
        select '1111-2222-3333' as island_code, 103 as peak_ccu, date '2026-11-11' as peak_day,
               3 as days_to_peak, 0.01 as slope_d1_7, null::double as half_life_days,
               false as traction_censored, 0.2 as retention_d1_mean,
               0.05 as retention_d7_mean, 30.0 as minutes_per_player_mean,
               date '2026-11-08' as anchor_day, 4 as obs_days""",
    "fortnite_events": f"""
        select '1111-2222-3333' as island_code, {T} as detected_at, 'tags_change' as event_type,
               '[]' as old_value, '[tycoon]' as new_value""",
    "fortnite_niche_opportunity": """
        select 'tycoon' as genre_unified, 300 as islands_listed, 120 as islands_active,
               5e4 as demand_w1, null::double as demand_growth,
               416.0 as demand_per_active_island, 40.0 as median_island_demand,
               0.7 as top10_share, null::double as opportunity_score""",
    "title_clones": f"""
        select * from (values ('roblox', '1', 'brainrot steal', 2, 1, 1,
                               {T} - interval 10 day, 1),
                              ('fortnite', '1111-2222-3333', 'brainrot steal', 2, 1, 1,
                               {T} - interval 5 day, 2))
        t(platform, id, clone_key, clone_group_size, clone_group_size_roblox,
          clone_group_size_fortnite, launched_at, clone_rank)""",
}


@pytest.fixture
def export_dir(tmp_path, monkeypatch):
    con = duckdb.connect()
    for name, sql in TABLES.items():
        # las tablas mensuales se publican partidas: probar también la unión
        fname = f"{name}_2026-11" if name == "roblox_snapshots" else name
        con.execute(f"copy ({sql}) to '{(tmp_path / fname).as_posix()}.parquet' "
                    "(format parquet)")
    monkeypatch.setenv("UGC_EXPORT_DIR", str(tmp_path))
    return tmp_path


def test_data_functions(export_dir):
    con = data.open_db(data.find_data_dir())
    assert data.freshness(con)["roblox_games"] == 2
    assert data.top_now(con, "roblox")["name"].tolist()[0] == "Old Obby"  # 247 > 147
    cycle = data.daily_cycle(con)
    assert set(cycle["platform"]) == {"Roblox", "Fortnite"}
    assert cycle.groupby("platform")["index"].mean().round(6).eq(100).all()
    la = data.launches(con, "roblox")
    assert la["id"].tolist() == ["1"]  # el 2 está censurado
    curves = data.launch_curves(con, "roblox", ["1"])
    # tracción hace 3 días; los snapshots del fixture cubren las últimas 47 h
    assert curves["days"].max() == pytest.approx(3) and curves["days"].min() > 1
    assert data.launch_curves(con, "fortnite", ["1111-2222-3333"])["days"].tolist() == [0, 1, 2, 3]
    found = data.search_games(con, "brainrot")
    assert set(found["platform"]) == {"roblox", "fortnite"}
    assert data.game_info(con, "roblox", "1")["creator"] == "Studio"
    assert data.game_info(con, "fortnite", "nope") is None
    assert len(data.game_charts(con, "1")) == 1
    assert len(data.game_events(con, "fortnite", "1111-2222-3333")) == 1
    fams = data.clone_families(con, 2, True)
    assert fams["family"].tolist() == ["brainrot steal"]
    assert len(data.clone_members(con, "brainrot steal")) == 2


PAGES = ["views/overview.py", "views/niches.py", "views/launches.py", "views/game.py",
         "views/clones.py"]


@pytest.mark.parametrize("page", PAGES)
def test_pages_render(export_dir, page):
    at = st_testing.AppTest.from_file("../dashboard/app.py", default_timeout=30)
    at.query_params["q"] = "brainrot"
    at.run()
    at.switch_page(page).run()
    assert not at.exception, [e.value for e in at.exception]


def test_missing_dataset_message(tmp_path, monkeypatch):
    monkeypatch.setenv("UGC_EXPORT_DIR", str(tmp_path))
    monkeypatch.setattr(data, "ROOT", tmp_path)
    at = st_testing.AppTest.from_file("../dashboard/app.py", default_timeout=30).run()
    assert "Dataset not found" in at.error[0].value

Live tracker of user-generated games on **Roblox** (experiences) and **Fortnite** (Creative / UEFN islands), collected automatically with GitHub Actions since November 2026 and updated weekly.

It was built to answer three questions: what makes a game grow fast (explosion) or keep its players (sustained), which niches have growing demand and few competitors, and whether the first 3–7 days of metrics can tell which new games will take off.

## What's inside

**Roblox** (public web APIs, current state only, so the time series is built by this project):
- `roblox_snapshots_YYYY-MM`: concurrent players, visits, favorites and votes per experience. Hot games every hour, warm daily, cold weekly.
- `roblox_chart_positions_YYYY-MM`: rank in every explore chart (Top Trending, Up-and-Coming, …), every hour.
- `roblox_search_positions_YYYY-MM`: daily keyword search sweep, used to find new launches before they take off.
- `roblox_games` / `roblox_games_history`: dimension (name, creator, genre, dates, …) with features, and its full change log.
- `roblox_media`, `roblox_gamepasses`, `roblox_events`: icon and thumbnails, game passes and prices, detected changes (name, description, update, icon, thumbnails).
- `roblox_creator_games`: every public experience of each tracked creator, with lifetime visits.

**Fortnite** (official Fortnite Data API, which keeps only 7 days of history; every island is queried at least weekly, so there are no gaps):
- `fortnite_snapshots_YYYY-MM`: daily metrics (plays, unique players, peak CCU, minutes per player, favorites, recommendations, D1/D7 retention) and hourly curves for active islands.
- `fortnite_islands` / `fortnite_islands_history`: the full public catalog (~290k islands) with features, and its change log.
- `fortnite_events`: islands added, removed or relisted, title and tag changes.

**Derived tables** (recomputed on every version):
- `*_daily` and `*_metrics`: daily series and curve metrics per game: days to peak, growth slopes over days 0–6 and 0–13 from first traction, half-life of the decline, sustained ratio (days 30–60 / peak), engagement and retention.
- `title_clones`: title-clone families across both platforms (e.g. every "Steal a Brainrot" copy).
- `fortnite_niche_opportunity` / `roblox_niche_opportunity`: weekly demand vs. number of competing games per genre (unique players on Fortnite, visits on Roblox), and on Roblox how much of it goes to games launched in the last 30 days.
- Features in the games tables: a unified Roblox/Fortnite genre (25 genres), title features (emojis, bracket tags like `[UPD]`, caps, keywords) and the creator's track record.

## Caveats

- **Tiers.** Games are tracked at a frequency that depends on their tier (hot / warm / cold, see `*_tier_history`). Series of cold games have gaps, and their peak is a lower bound.
- **Survival bias.** New games that never take off are also tracked on purpose. The game tables keep every game, not only the hits.
- **Censoring.** Most popular games already had traction when tracking started. Their launch timing is unknown, and `traction_censored` marks them. To study launches, filter `traction_censored = false`.
- **Fortnite thresholds.** The Fortnite API reports nothing for buckets with fewer than 5 unique players, so a missing day means fewer than 5 players.
- **Timestamps.** All timestamps are UTC, and `snapshot_at` is the real request time.
- **Early weeks.** Week-over-week growth and the opportunity score need two weeks of coverage, so they are empty in the first versions.

## Source code

Collectors, tests and the full methodology: https://github.com/Agustin-Molina/ugc-game-analytics

Data comes from public endpoints of Roblox and Epic Games. This dataset is not affiliated with or endorsed by either company. It is meant for research and education (CC BY-NC 4.0).

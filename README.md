# f1 data analysis with Motherduck's Flights & Dives

## Purpose
Explore the capabilities of Motherduck's flights and dives tooling as well as indulge my interest in F1!

## What do you get?
An interactive dashboard or ["dive"](https://app.motherduck.com/dives) that allows you to look at how driver's lap times compared to each other over the course of the 2025 season. 

<img width="889" height="828" alt="Screenshot 2026-07-29 at 3 22 50 PM" src="https://github.com/user-attachments/assets/2353af94-9dcd-4860-933a-4b1183351a48" />

## How does it work?
Data is collected from the [OpenF1 API](https://openf1.org/) in a Flight then transformed using dbt Core.

The pipeline is two [MotherDuck Flights](https://motherduck.com/docs/concepts/flights/) — scheduled Python programs that run in the MotherDuck cloud — chained together, followed by a Dive for visualization:

1. **`f1-ingest-openf1`** ([flights/ingest_openf1](flights/ingest_openf1)) runs every Tuesday at 06:00 UTC. It pulls the season's Race sessions, meetings, drivers, and laps from the OpenF1 API and loads them into `f1.raw.*` tables. Each run deletes and re-inserts rows for the sessions it fetched, so it's idempotent and picks up corrections OpenF1 publishes after the fact.
2. **`f1-transform-dbt`** ([flights/transform_dbt](flights/transform_dbt)) runs 30 minutes later, at 06:30 UTC, so it always sees that day's ingested data. It executes `dbt build` against `md:f1` using the [dbt](dbt) project in this repo, which stages the raw OpenF1 tables and builds a `fct_lap_pace` mart: each driver's lap time compared to the fastest and median lap turned on that same lap of that same session. At build time, this Flight downloads the `dbt/` project straight from GitHub (a tarball of the `main` branch), so edits under `dbt/` take effect on the next run without needing to regenerate or push the Flight's source.
3. The [Dive](https://motherduck.com/docs/category/dives/) ([dives/season_pace_dive.tsx](dives/season_pace_dive.tsx)) is a React data app that queries `fct_lap_pace` live to render the driver-comparison chart shown above. [.dive-preview](.dive-preview) is a local Vite app (see `.claude/launch.json`) for previewing Dive changes before pushing them.
4. **`f1-race-pace-email`** ([flights/race_pace_email](flights/race_pace_email)) is an on-demand Flight that emails the same race-detail data and metrics shown in the Dive — race header, driver avg lap time / cumulative gap-to-fastest table, and a top-5 gap-to-fastest chart — for the most recent Race session. It sends via Gmail SMTP and requires a MotherDuck Flight secret named `gmail_smtp` (params `SMTP_USERNAME`, `SMTP_PASSWORD`).

Flights can be managed via SQL or MCP — see the [Flights SQL reference](https://motherduck.com/docs/sql-reference/motherduck-sql-reference/flights/) and [Dives SQL reference](https://motherduck.com/docs/sql-reference/motherduck-sql-reference/dives/) for details.

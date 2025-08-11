# Sprint dbt Project

## 🗂️ Overview

This folder contains all **dbt models**, seeds, macros, and tests for the Sprint analytics pipeline.  
The transformations here take raw game telemetry from DuckDB (`sprint_raw`) and produce clean, analysis-ready marts in `sprint_mart`.

---

## 📦 Structure

```bash
models/
├── staging/
│ ├── event_heartbeat.sql # Unpacks player heartbeat JSON
│ ├── stage_centroids.sql # Computes per-player centroid positions
│ ├── stage_encounters.sql # Detects close encounters from heartbeat data
│ ├── schema.yml # Contracts & tests for staging models
│
├── marts/
│ ├── country_monthly_playtime.sql
│ ├── country_weekly_revenue.sql
│ ├── encounter_summary_daily.sql
│ ├── player_activity_daily.sql
│ ├── player_consecutive_days_monthly.sql
│ ├── player_stats_lifetime.sql
│ ├── session_close_encounters_daily.sql
│ ├── schema.yml # Contracts & tests for marts

seeds/
├── dim_products.csv # Static product dimension

macros/
├── compute_encounters.sql # Reusable spatial/time gap logic

tests/
├── no_zero_duration_encounters.sql # Data quality test example
```

---

## 🧠 Key Logic

- **Staging models** clean and standardize source events from `sprint_raw` into `sprint_stage`.
- **Encounter detection** uses `compute_encounters` macro to apply spatial proximity (≤ 50 units) and time gap (> 3 minutes) rules.
- **Mart models** roll up data to player, session, and country grains for downstream analysis.

---

## 🚀 Usage

From the project root:

```bash
dbt deps
dbt seed --select dim_products
dbt run
dbt test
```

---

## 📊 Target Schemas

**sprint_stage**

- event_heartbeat
- fact_session
- stage_centroids
- stage_encounters

**sprint_mart**

- country_monthly_playtime
- country_weekly_revenue
- encounter_summary_daily
- player_activity_daily
- player_consecutive_days_monthly
- player_stats_lifetime
- session_close_encounters_daily

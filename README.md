# Welcome to Sprint

## 🕹️ Project Overview

We're simulating an analytics infrastructure for a new global online game targeting 20M players over two years. This take-home project demonstrates how to design and implement an end-to-end analytics pipeline, from synthetic data generation to modeling and insights, aligned with real-world analytical use cases like player retention, in-game monetization, and session-level behaviors.

The project focuses on:

- Simulating gameplay events using realistic mock data
- Designing an ingestion and transformation pipeline
- Modeling an analytics warehouse
- Enabling performant, business-critical queries

---

## 📦 Pipeline Overview

| Stage         | Tooling         | Description                                                                                                                           |
| ------------- | --------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| Data Gen      | `main.py`       | Orchestrates all generators (sessions, transactions, products, heartbeats) to produce realistic JSON/CSV event data                   |
| Ingest & Load | Python + DuckDB | Loads generated raw event data into `sprint_raw` tables in DuckDB                                                                     |
| Transform     | dbt             | Transforms raw data through `sprint_stage` and `sprint_dim` into `sprint_mart` models using game-aware logic (e.g., close encounters) |
| Analysis      | SQL + Jupyter   | Runs performant analytical queries and produces insights from `sprint_mart` tables                                                    |

---

## 🧪 Simulated Event Sources

We simulate three primary logging sources reflecting real-time game telemetry:

1. **Session Ends**  
   PlayerId, SessionId, EventDateTime, Country, EventLengthSeconds, Kills, Deaths

2. **In-Game Purchases**  
   TransactionId, PlayerId, EventDateTime, PurchaseItem, PurchasePrice, Currency, IsRecurring, Cycle, TransactionType (Battle Pass, Skins, Emotes)

3. **Player Heartbeats**  
   PlayerId, EventDateTime, TeamId, SessionId, PositionX, PositionY, PositionZ

Each player plays at most 1 session per day due to computation constraints. Heartbeats are generated every 30s during active sessions. Purchase behavior varies by player cluster.

---

## 🧠 Analytics Goals

The data model and pipeline are built to enable analysts to answer:

- Daily/weekly/monthly player activity trends
- Player lifecycle metrics (kills/deaths, revenue, streaks)
- Engagement by region
- Real-time game dynamics like **close encounters**

Close encounters are derived from heartbeat data in dbt using spatial proximity and time gap logic.

---

## ⚙️ How To Use Locally

Follow these steps to clone the repo, generate synthetic data, and run the full dbt analytics pipeline on your machine:

```bash
# 1. Clone the repository
git clone <repo-url>
cd repo

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Generate all synthetic data (players, sessions, transactions, etc.)
python scripts/main.py --entrypoint all

# 4. Run dbt to build models and tests
dbt deps
dbt run
dbt test

# 5. Generate and serve dbt documentation site
dbt docs generate
dbt docs serve
```
💡 Notes
[ ] This setup assumes you have Python and dbt installed and available in your PATH.  
[ ] All generated data is loaded into the local DuckDB database configured in the project.  
[ ] You can adjust data generation by specifying different --entrypoint options to main.py.  

To stop serving docs, press Ctrl+C in the terminal running dbt docs serve.

## 🗂️ Folder Structure

```bash
sprint/                         # Root project directory
│
├── data/                       # Output folder for synthetic JSON/CSV/Parquet data
│   ├── sessions/                # JSON dumps for player sessions
│
├── scripts/                    # CLI entry points for the pipeline
│   ├── main.py                  # Orchestrates all data generation and ingestion
│
├── src/                        # Core simulation logic
│   ├── __init__.py
│   ├── session_generator.py     # Creates player sessions and metadata
│   ├── heartbeat_generator.py   # Simulates player movement heartbeats in 3D space
│   ├── transaction_generator.py # Simulates in-game purchase transactions
│   ├── loader.py                # Loads generated data into DuckDB
│   ├── summarizer.py            # Aggregates kills, deaths, session stats
│   ├── utils.py                 # Shared helper functions
│   ├── movement/                # Movement function implementations
│   │   ├── __init__.py
│   │   ├── step/
│   │   │   ├── lorentzian.py
│   │   │   ├── bezier.py
│   │   │   ├── lissajous.py
│   │   │   ├── perlin.py
│   │   │   └── ...
│
├── dbt_project/                 # dbt transformations
│   ├── seeds/                   # Static ref data from product_generator.py
│   │   ├── dim_products.csv
│   │
│   ├── models/
│   │   ├── staging/
│   │   │   ├── event_heartbeat.sql
│   │   │   ├── stage_centroids.sql
│   │   │   ├── stage_encounters.sql
│   │   │   ├── schema.yml
│   │
│   │   ├── marts/
│   │   │   ├── country_monthly_playtime.sql
│   │   │   ├── country_weekly_revenue.sql
│   │   │   ├── encounter_summary_daily.sql
│   │   │   ├── player_activity_daily.sql
│   │   │   ├── player_consecutive_days_monthly.sql
│   │   │   ├── player_stats_lifetime.sql
│   │   │   ├── session_close_encounters_daily.sql
│   │   │   ├── schema.yml
│
│   ├── macros/
│   │   ├── compute_encounters.sql
│
│   ├── tests/
│   │   ├── no_zero_duration_encounters.sql
│
├── tests/                       # Pytest unit tests
│   ├── test_db.py
│   ├── test_products.py
│   ├── test_sessions.py
│   ├── test_transactions.py
│
├── notebooks/                   # Analysis notebooks
│   ├── analysis.ipynb
│   ├── player_paths.ipynb
│
├── queries/                     # Standalone SQL scripts for the 7 business questions
│
├── requirements.txt
├── README.md
└── LICENSE
```

---

## 🧱 Data Warehouse Design

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python&logoColor=white&style=flat-square) |
![DuckDB](https://img.shields.io/badge/DuckDB-%231C2D3F?logo=DuckDB&logoColor=white&style=flat-square) |
![dbt](https://img.shields.io/badge/dbt-%23FF694B?logo=dbt&logoColor=white&style=flat-square)

**Schema & Table Catalog**

**sprint_dim**

- dim_players
- dim_products

**sprint_mart**

- country_monthly_playtime
- country_weekly_revenue
- encounter_summary_daily
- player_activity_daily
- player_consecutive_days_monthly
- player_stats_lifetime
- session_close_encounters_daily

**sprint_raw**

- event_session
- event_signons
- event_transaction

**sprint_stage**

- event_heartbeat
- fact_session
- stage_centroids
- stage_encounters

---

## 📊 Analysis

Analysis is performed in Jupyter notebooks and SQL, targeting `sprint_mart` tables to answer gameplay and business questions with performant queries.

---

## 🚀 Coming Up Next

- [x] End-to-end pipeline: data generation → ingestion → transformation → marts
- [x] dbt models for encounters, centroids, and session facts
- [x] Analytical queries for business/gameplay metrics
- [ ] Better player generation — modeling churn, retention, and realistic growth
- [ ] Player profiling — generate realistic player metadata with Faker
- [ ] Machine learning — predictive analytics (random forest, logistic regression, XGBoost) on player purchase behavior to identify targetable sales segments

## 📣 Stay Connected

[![Read on Medium](https://img.shields.io/badge/Read%20on-Medium-black?logo=medium)](https://upandtothewrite.medium.com/)
[![Find Me on LinkedIn](https://img.shields.io/badge/Connect-LinkedIn-blue?logo=linkedin)](https://www.linkedin.com/in/chrisadan/)

---

> 📌 **Note**: The take-home brief for this project was originally provided by Bungie, Inc. as part of a job application process. All code, implementation details, and architectural decisions are original work created by the author for demonstration purposes only.

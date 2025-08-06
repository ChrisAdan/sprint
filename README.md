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

| Stage           | Tooling                   | Description                                                                 |
| --------------- | ------------------------- | --------------------------------------------------------------------------- |
| Data Gen        | `generate_sample_data.py` | Creates realistic JSON/CSV event files for Sessions, Purchases, Heartbeats  |
| Ingest & Load   | Python + DuckDB           | Loads mock event data into staging tables (raw format)                      |
| Transform       | dbt                       | Builds STG & DIM/FACT models with game-aware logic (e.g., close encounters) |
| Analysis        | SQL + dbt                 | Answers key gameplay/business questions via performant queries              |
| Dashboard (opt) | Streamlit                 | Lightweight app to visualize insights locally                               |

---

## 🧪 Simulated Event Sources

We simulate three primary logging sources reflecting real-time game telemetry:

1. **Session Ends**  
   PlayerId, SessionId, Timestamp, Country, EventLength, Kills, Deaths

2. **In-Game Purchases**  
   PlayerId, Timestamp, Item, Price

3. **Player Heartbeats**  
   PlayerId, Timestamp, TeamId, SessionId, PositionX/Y/Z

Each player plays 0–10 sessions/day probabilistically. Heartbeats are generated every 30s during active sessions. Purchase behavior varies by player cluster.

---

## 🧠 Analytics Goals

The data model and pipeline are built to enable analysts to answer:

- Daily/weekly/monthly player activity trends
- Player lifecycle metrics (kills/deaths, revenue, streaks)
- Engagement by region
- Real-time game dynamics like **close encounters**

Close encounters are derived from heartbeat data in dbt using spatial proximity and time gap logic.

---

## 🗂️ Folder Structure

```bash
bungie_analytics/
├── data/ # Output folder for synthetic data
├── scripts/
│ ├── generate_sample_data.py # Python script to generate JSON/CSV payloads
│ ├── ingest_to_duckdb.py # Load raw data into DuckDB
├── dbt_project/ # dbt models and transformations
│ ├── models/
│ │ ├── stg_*.sql
│ │ ├── fact_sessions.sql
│ │ ├── fact_encounters.sql
│ │ ├── dim_players.sql
│ │ └── ...
├── queries/ # SQL files answering the 7 questions
├── streamlit_app.py # Optional dashboard
├── README.md
└── LICENSE
```

---

## 🧱 Data Warehouse Design

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python&logoColor=white&style=flat-square) |
![DuckDB](https://img.shields.io/badge/DuckDB-%231C2D3F?logo=DuckDB&logoColor=white&style=flat-square) |
![dbt](https://img.shields.io/badge/dbt-%23FF694B?logo=dbt&logoColor=white&style=flat-square)

### STG Layer

- `stg_sessions`
- `stg_purchases`
- `stg_heartbeats`

### DIM / FACT Models

- `dim_players` — Player profile and derived attributes
- `fact_sessions` — Normalized session-level stats
- `fact_encounters` — Derived from heartbeats using Euclidean distance + time logic
- `fact_purchases` — Aggregated in-game purchases
- `date_spine` — Helps with time-aware rollups and streak detection

---

## 📊 Optional: Streamlit Dashboard

An optional `streamlit_app.py` renders graphs and stats:

- Playtime and K/D ratios
- Weekly player activity heatmaps
- Revenue per region
- Encounter metrics per session

This is not required per the brief but demonstrates the data pipeline's usability for analysts or PMs.

---

## 🚀 Coming Up Next

- [ ] Implement `generate_sample_data.py` with realism (players, country dist, team composition)
- [ ] Define table creation & load in `ingest_to_duckdb.py`
- [ ] Write dbt models (starting with STG → DIM → FACT)
- [ ] Derive close encounters in `fact_encounters`
- [ ] Write SQL answers for all 7 questions
- [ ] Estimate table growth in documentation
- [ ] (Optional) Add Streamlit dashboard

## 📣 Stay Connected

[![Read on Medium](https://img.shields.io/badge/Read%20on-Medium-black?logo=medium)](https://upandtothewrite.medium.com/)
[![Find Me on LinkedIn](https://img.shields.io/badge/Connect-LinkedIn-blue?logo=linkedin)](https://www.linkedin.com/in/chrisadan/)

---

> 📌 **Note**: The take-home brief for this project was originally provided by Bungie, Inc. as part of a job application process. All code, implementation details, and architectural decisions are original work created by the author for demonstration purposes only.

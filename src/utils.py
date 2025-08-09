import random
from pathlib import Path

import numpy as np
import pandas as pd

# ==== Constants ====
GRID_BOUNDS = (-100, 100)
HEARTBEAT_INTERVAL = 30  # seconds
SESSION_MAX_DURATION_SECONDS = 1800
COUNTRIES = ['US', 'BR', 'MX', 'FR', 'ES', 'DE']
SESSION_PATH = Path("data/sessions")
SESSION_PATH.mkdir(parents=True, exist_ok=True)
AVG_DAILY_SESSIONS = 10

MIN_TEAMS = 1
MAX_TEAMS = 5
MIN_PLAYERS_PER_TEAM = 1
MAX_PLAYERS_PER_TEAM = 2

RANDOM_SEED = 42

def generate_player_ids(n_players=1000, seed=RANDOM_SEED):
    random.seed(seed)
    np.random.seed(seed)
    ids = np.random.choice(range(n_players), size=n_players, replace=False)
    return [str(i).zfill(4) for i in ids]

def model_sign_ons(player_ids, n_days=365, start_date="2025-01-01", seed=RANDOM_SEED):
    random.seed(seed)
    np.random.seed(seed)

    start_dt = pd.to_datetime(start_date)
    all_dates = pd.date_range(start=start_dt, periods=n_days)
    patterns = ['daily', 'weekday', 'cyclical', 'decay']
    behaviors = {pid: random.choice(patterns) for pid in player_ids}

    records = []
    for pid in player_ids:
        behavior = behaviors[pid]
        for i, date in enumerate(all_dates):
            day = date.weekday()
            x = i / n_days
            p = {
                'daily': 0.9,
                'weekday': 0.8 if day < 5 else 0.3,
                'cyclical': 0.5 + 0.4 * np.sin(2 * np.pi * x * 4),
                'decay': max(0.9 - x, 0.1)
            }[behavior]
            if np.random.rand() < p:
                records.append({"playerId": pid, "date": date.date()})
    return pd.DataFrame(records)

def assign_countries(player_ids, seed=42):
    np.random.seed(seed)
    countries = np.random.choice(COUNTRIES, size=len(player_ids))
    return dict(zip(player_ids, countries))

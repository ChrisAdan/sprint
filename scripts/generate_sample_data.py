import numpy as np
import pandas as pd
import random
import json

RANDOM_SEED=42

def generate_player_ids(n_players=1000, seed=RANDOM_SEED):
    random.seed(seed)
    np.random.seed(seed)
    
    # Generate unique 4-digit IDs as strings
    ids = np.random.choice(range(n_players), size=n_players, replace=False)
    player_ids = [str(i).zfill(4) for i in ids]
    return player_ids

def model_sign_ons(player_ids, n_days=365, start_date="2025-01-01", seed=RANDOM_SEED):
    np.random.seed(seed)
    random.seed(seed)

    start_dt = pd.to_datetime(start_date)
    all_dates = pd.date_range(start=start_dt, periods=n_days)

    behavior_patterns = ['daily', 'weekday', 'cyclical', 'decay']
    player_behaviors = {
        pid: random.choice(behavior_patterns) for pid in player_ids
    }

    records = []

    for pid in player_ids:
        behavior = player_behaviors[pid]
        for i, date in enumerate(all_dates):
            day_of_week = date.weekday()  # Monday=0, Sunday=6
            x = i / n_days  # normalized time (0 to 1)
            p = 0.0  # base probability

            # Choose pattern
            if behavior == 'daily':
                p = 0.9  # near-daily
            elif behavior == 'weekday':
                p = 0.8 if day_of_week < 5 else 0.3
            elif behavior == 'cyclical':
                p = 0.5 + 0.4 * np.sin(2 * np.pi * x * 4)  # 4 cycles/year
            elif behavior == 'decay':
                p = max(0.9 - x, 0.1)  # linear decay over year

            signed_in = np.random.rand() < p
            if signed_in:
                records.append({
                    "player_id": pid,
                    "date": date.date(),
                })

    signins_df = pd.DataFrame(records)
    return signins_df


def main():
    player_ids = generate_player_ids(n_players=1000)
    signins_df = model_sign_ons(player_ids, n_days=365)

    print(signins_df.head())
    print(f"Total player-day sign-ins: {len(signins_df)}")

if __name__ == '__main__':
    main()
import random
from datetime import datetime
import pandas as pd

from loader import save_transactions_json, write_transactions_to_duckdb

# Behavior buckets config
BEHAVIOR_BUCKETS = {
    "no_purchase": {"prob": 0.65},  # 60–70% of players
    "minnow": {"prob": 0.25, "min_purchases": 1, "max_purchases": 2, "min_amount": 1, "max_amount": 10},
    "whale": {"prob": 0.05, "min_purchases": 2, "max_purchases": 5, "min_amount": 10, "max_amount": 50},
}

def assign_behavior():
    """Randomly assigns a purchase behavior bucket according to defined probabilities."""
    r = random.random()
    cumulative = 0
    for bucket, config in BEHAVIOR_BUCKETS.items():
        cumulative += config["prob"]
        if r <= cumulative:
            return bucket
    return "no_purchase"  # fallback


def generate_transactions_for_player_day(player_id, date_str, products_df):
    """Generate transactions list for a player on a specific day."""
    bucket = assign_behavior()
    if bucket == "no_purchase":
        return []

    cfg = BEHAVIOR_BUCKETS[bucket]
    num_purchases = random.randint(cfg["min_purchases"], cfg["max_purchases"])
    transactions = []

    for _ in range(num_purchases):
        product = products_df.sample(1).iloc[0]
        amount = round(random.uniform(cfg["min_amount"], cfg["max_amount"]), 2)
        ts = datetime.strptime(date_str, "%Y-%m-%d") + pd.to_timedelta(random.randint(0, 86399), unit="s")

        transactions.append({
            "transactionId": f"TX-{player_id[:8]}-{ts.strftime('%Y%m%d%H%M%S')}-{random.randint(1000,9999)}",
            "playerId": player_id,
            "eventDateTime": ts.isoformat(),
            "purchaseItem": product["productSku"],
            "purchasePrice": amount,  # USD
            "purchaseQuantity": product["quantity"],
            "currency": "USD",
            "isRecurring": product["isRecurring"],
            "cycle": product["cycle"],
            "transactionType": product["transactionType"]
        })
    return transactions


def generate_transactions(signins_df, products_df, conn):
    """
    Generate transactions for all player/day combos and write to disk + DuckDB.

    Args:
        signins_df (pd.DataFrame): must have columns ['playerId', 'date']
        products_df (pd.DataFrame): dim_products catalog
        conn (duckdb.DuckDBPyConnection)
    """
    grouped = signins_df.groupby("date")
    for date_str, day_df in grouped:
        all_transactions = []
        for _, row in day_df.iterrows():
            player_id = row["playerId"]
            txs = generate_transactions_for_player_day(player_id, date_str, products_df)
            all_transactions.extend(txs)

        # Save to disk json + write to DuckDB via helpers
        save_transactions_json(date_str, all_transactions)
        write_transactions_to_duckdb(all_transactions, conn)

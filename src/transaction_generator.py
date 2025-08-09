import random
from datetime import datetime
import pandas as pd

from loader import write_dataframe_to_table
from utils import convert_numpy_types

# Behavior buckets config
BEHAVIOR_BUCKETS = {
    "no_purchase": {"prob": 0.65},  # 60–70% of players
    "minnow": {"prob": 0.25, "min_purchases": 1, "max_purchases": 2, "min_amount": 1, "max_amount": 10},
    "whale": {"prob": 0.05, "min_purchases": 2, "max_purchases": 5, "min_amount": 10, "max_amount": 50},
}

def assign_behavior():
    """Randomly assign purchase behavior bucket based on defined probabilities."""
    r = random.random()
    cumulative = 0
    for bucket, config in BEHAVIOR_BUCKETS.items():
        cumulative += config["prob"]
        if r <= cumulative:
            return bucket
    return "no_purchase"  # fallback

def normalize_cycle_and_recurring(product):
    """
    Only battlepasses can have a monthly/yearly cycle.
    For all other products, force one-time purchase.
    """
    if product["transactionType"].lower() == "battlepass":
        return product["isRecurring"], product["cycle"]
    else:
        return False, ''

def generate_transactions_for_player_day(player_id, date, products_df):
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
        ts = pd.to_datetime(date) + pd.to_timedelta(random.randint(0, 86399), unit="s")

        is_recurring, cycle = normalize_cycle_and_recurring(product)

        transactions.append({
            "transactionId": f"TX-{player_id[:8]}-{ts.strftime('%Y%m%d%H%M%S')}-{random.randint(1000,9999)}",
            "playerId": player_id,
            "eventDateTime": ts.isoformat(),
            "purchaseItem": product["productSku"],
            "purchasePrice": amount,  # USD
            "purchaseQuantity": product["quantity"],
            "currency": "USD",
            "isRecurring": is_recurring,
            "cycle": cycle,
            "transactionType": product["transactionType"]
        })
    return transactions

def generate_transactions(signins_df, products_df, duck_conn):
    """
    Generate transactions for all player/day combos and write batch data to DuckDB.

    Args:
        signins_df (pd.DataFrame): must have columns ['playerId', 'date']
        products_df (pd.DataFrame): dim_products catalog
        duck_conn (duckdb.DuckDBPyConnection)
    """
    grouped = signins_df.groupby("date")
    for date, day_df in grouped:
        all_transactions = []
        for _, row in day_df.iterrows():
            player_id = row["playerId"]
            txs = generate_transactions_for_player_day(player_id, date, products_df)
            all_transactions.extend(txs)

        if not all_transactions:
            continue

        df_tx = pd.DataFrame(all_transactions)

        # Convert numpy types to native Python types before write
        df_tx = df_tx.map(convert_numpy_types)
        print(f'saving {len(df_tx)} to csv')
        df_tx.to_csv('test.csv', index=False)

        print(f'Writing transactions for {date}')

        write_dataframe_to_table(
            duck_conn=duck_conn,
            schema="raw",
            table="event_transaction",
            df=df_tx,
            primary_key="transactionId",
            replace=False,  # append daily batches
        )

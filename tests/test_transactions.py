import pytest
import pandas as pd
import duckdb
from datetime import datetime

import transaction_generator as tg  # for transaction_generator functions
from loader import save_transactions_json, write_transactions_to_duckdb  # for loader functions


@pytest.fixture
def sample_products_df():
    data = {
        "productId": [1, 2, 3],
        "productSku": ["SKU-1001", "SKU-1002", "SKU-1003"],
        "quantity": [1, 2, 1],
        "purchasePrice": [9.99, 14.99, 4.99],
        "isRecurring": [True, False, False],
        "cycle": ["M", "", ""],
        "transactionType": ["BattlePass", "Emote", "Skin"],
        "tier": ["Premium", "Standard", "Standard"],
        "productName": ["BP Premium", "Cool Emote", "Rare Skin"],
        "createdAt": ["2025-01-01T00:00:00Z"] * 3,
        "lastModifiedAt": ["2025-01-01T00:00:00Z"] * 3,
    }
    return pd.DataFrame(data)


def test_assign_behavior_distribution():
    counts = {"no_purchase": 0, "minnow": 0, "whale": 0}
    for _ in range(10000):
        b = tg.assign_behavior()
        counts[b] += 1
    assert 0.6 < counts["no_purchase"] / 10000 < 0.7
    assert 0.2 < counts["minnow"] / 10000 < 0.3
    assert 0.03 < counts["whale"] / 10000 < 0.07


def test_generate_transactions_for_player_day_no_purchase(sample_products_df):
    player_id = "player_1"
    date_str = "2025-08-08"

    tg.assign_behavior = lambda: "no_purchase"
    txs = tg.generate_transactions_for_player_day(player_id, date_str, sample_products_df)
    assert txs == []


@pytest.mark.parametrize("behavior", ["minnow", "whale"])
def test_generate_transactions_for_player_day_behavior(sample_products_df, behavior):
    player_id = "player_42"
    date_str = "2025-08-08"

    tg.assign_behavior = lambda: behavior
    txs = tg.generate_transactions_for_player_day(player_id, date_str, sample_products_df)

    assert isinstance(txs, list)
    assert tg.BEHAVIOR_BUCKETS[behavior]["min_purchases"] <= len(txs) <= tg.BEHAVIOR_BUCKETS[behavior]["max_purchases"]

    for tx in txs:
        for key in [
            "transactionId",
            "playerId",
            "eventDateTime",
            "purchaseItem",
            "purchasePrice",
            "purchaseQuantity",
            "currency",
            "isRecurring",
            "cycle",
            "transactionType",
        ]:
            assert key in tx

        assert tx["playerId"] == player_id

        amt = tx["purchasePrice"]
        assert tg.BEHAVIOR_BUCKETS[behavior]["min_amount"] <= amt <= tg.BEHAVIOR_BUCKETS[behavior]["max_amount"]

        assert tx["currency"] == "USD"
        assert tx["purchaseItem"] in sample_products_df["productSku"].values

        datetime.fromisoformat(tx["eventDateTime"])
        assert tx["transactionId"].startswith("TX-")


def test_save_transactions_json_creates_file(tmp_path):
    txs = [
        {
            "transactionId": "TX-1",
            "playerId": "p1",
            "eventDateTime": "2025-08-08T12:00:00",
            "purchaseItem": "SKU-1001",
            "purchasePrice": 5.0,
            "purchaseQuantity": 1,
            "currency": "USD",
            "isRecurring": False,
            "cycle": "",
            "transactionType": "Skin",
        }
    ]
    save_transactions_json("2025-08-08", txs, out_dir=tmp_path)
    file_path = tmp_path / "transactions_20250808.json"
    assert file_path.exists()


def test_write_transactions_to_duckdb_inserts(sample_products_df):
    conn = duckdb.connect(database=":memory:")

    txs = [
        {
            "transactionId": "TX-1",
            "playerId": "p1",
            "eventDateTime": "2025-08-08T12:00:00",
            "purchaseItem": "SKU-1001",
            "purchasePrice": 5.0,
            "purchaseQuantity": 1,
            "currency": "USD",
            "isRecurring": False,
            "cycle": "",
            "transactionType": "Skin",
        }
    ]

    write_transactions_to_duckdb(txs, conn)

    result = conn.execute("SELECT * FROM raw.event_transactions").fetchall()
    assert len(result) == 1
    assert result[0][0] == "TX-1"


def test_generate_transactions_runs_without_error(sample_products_df):
    conn = duckdb.connect(database=":memory:")
    df = pd.DataFrame(
        {
            "playerId": ["p1", "p2"],
            "date": ["2025-08-08", "2025-08-08"],
        }
    )
    tg.generate_transactions(df, sample_products_df, conn)

"""
generate_products.py

Generates a synthetic dim_products.csv seed file in USD.
This script is idempotent: running it again will overwrite the file.
"""

import os
import csv
import random
from datetime import datetime, timezone

# Configuration
NUM_PRODUCTS = 40  # or random.randint(30, 50)
OUTPUT_PATH = os.path.join(
    os.path.dirname(__file__), "..", "dbt_project", "seeds", "dim_products.csv"
)

TIERS = ["Premium", "Standard"]
TRANSACTION_TYPES = ["BattlePass", "Emote", "Skin"]


def generate_products(n):
    """
    Generate n synthetic product records as a list of dicts.

    Each record includes:
    - productId, productSku
    - tier, quantity, purchasePrice (USD)
    - transactionType, isRecurring, cycle
    - createdAt, lastModifiedAt timestamps (UTC ISO8601)
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    products = []
    for i in range(n):
        is_recurring = random.choice([True, False])
        cycle = random.choice(["M", "Y"]) if is_recurring else ""
        products.append({
            "productId": i + 1,
            "productSku": f"SKU-{1000 + i}",
            "tier": random.choice(TIERS),
            "quantity": random.randint(1, 10),
            "purchasePrice": round(random.uniform(1.99, 99.99), 2),
            "transactionType": random.choice(TRANSACTION_TYPES),
            "isRecurring": is_recurring,
            "cycle": cycle,
            "createdAt": now_iso,
            "lastModifiedAt": now_iso
        })
    return products


def write_products_to_csv(products, output_path):
    """
    Write product list to CSV, overwriting any existing file.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, mode="w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=products[0].keys())
        writer.writeheader()
        writer.writerows(products)


if __name__ == "__main__":
    products = generate_products(NUM_PRODUCTS)
    write_products_to_csv(products, OUTPUT_PATH)
    print(f"✅ Generated {len(products)} products in {OUTPUT_PATH}")

import argparse
import uuid
import numpy as np
import pandas as pd

from utils import (
    generate_player_ids,
    model_sign_ons,
    assign_countries,
    DIM_PRODUCTS_CSV,
    DEFAULT_STARTING_PLAYERS,
)
from session_generator import generate_sessions
from loader import (
    connect_to_duckdb,
    load_table_to_df,
    write_dataframe_to_table,
    clear_old_data,
)
from product_generator import generate_products, write_products_to_csv, OUTPUT_PATH
from transaction_generator import generate_transactions


def prompt_yes_no(message):
    while True:
        resp = input(f"{message} [y/n]: ").strip().lower()
        if resp in ("y", "yes"):
            return True
        if resp in ("n", "no"):
            return False
        print("Please enter 'y' or 'n'.")


def generate_new_player_ids(n, existing_ids_set):
    """Generate n new unique player IDs avoiding collisions."""
    new_ids = []
    while len(new_ids) < n:
        pid = str(uuid.uuid4())
        if pid not in existing_ids_set:
            new_ids.append(pid)
            existing_ids_set.add(pid)
    return new_ids


def run_players(conn, days=365, initial_players=1000, daily_growth_rate=0.001, daily_decay_rate=0.0007):
    """
    Generate players modeling conservative growth and decay over a period,
    unless user opts to reuse existing players.
    """
    existing_players_df = load_table_to_df(conn, "raw", "dim_players")
    if existing_players_df is not None and not existing_players_df.empty:
        print(f"⚠️ Found {len(existing_players_df)} existing players in DuckDB.")
        if prompt_yes_no("Do you want to reuse the existing players instead of regenerating?"):
            return existing_players_df["playerId"].tolist()
        else:
            print("❗ Clearing players and downstream data...")
            clear_old_data(conn, level="players")

    existing_ids_set = set()
    # Start with initial base players
    base_player_ids = generate_player_ids(initial_players)
    existing_ids_set.update(base_player_ids)

    daily_active_players = set(base_player_ids)
    all_player_ids = set(base_player_ids)

    for day in range(days):
        current_population = len(daily_active_players)
        net_change = int(current_population * (daily_growth_rate - daily_decay_rate))

        if net_change > 0:
            new_ids = generate_new_player_ids(net_change, existing_ids_set)
            daily_active_players.update(new_ids)
            all_player_ids.update(new_ids)
        elif net_change < 0:
            churn_count = abs(net_change)
            if churn_count > current_population:
                churn_count = current_population
            churned = set(np.random.choice(list(daily_active_players), churn_count, replace=False))
            daily_active_players.difference_update(churned)

    df_players = pd.DataFrame({"playerId": list(all_player_ids)})
    write_dataframe_to_table(conn, "dim", "dim_players", df_players, primary_key="playerId", replace=True)

    print(f"✅ Generated {len(all_player_ids)} total players with growth and decay over {days} days.")
    return list(all_player_ids)


def run_products():
    while True:
        try:
            n_products = int(input("Enter number of products to simulate (30-50 recommended): ").strip())
            if n_products <= 0:
                print("Please enter a positive integer.")
                continue
            break
        except ValueError:
            print("❌ Invalid number entered.")

    print("🛍 Generating product dimension seed file...")
    products = generate_products(n_products)
    write_products_to_csv(products, OUTPUT_PATH)
    print(f"✅ {n_products} products generated and saved to seeds in {OUTPUT_PATH}.")


def run_signons(conn, player_ids):
    signons_df = load_table_to_df(conn, "raw", "event_signons")
    if signons_df is not None:
        print(f"⚠️ Found existing sign-ons data with {len(signons_df)} records in DuckDB.")
        if prompt_yes_no("Do you want to use the existing sign-ons instead of regenerating?"):
            return signons_df, assign_countries(player_ids)

    print("📅 Modeling player sign-ons...")
    signons_df = model_sign_ons(player_ids, n_days=365)
    write_dataframe_to_table(conn, "raw", "event_signons", signons_df, replace=True)
    return signons_df, assign_countries(player_ids)


def run_sessions(conn, signons_df, country_map):
    print("🎮 Generating sessions and inserting into DuckDB...")
    generate_sessions(signons_df, country_map, conn)


def run_transactions(conn, signons_df):
    print("💸 Generating transactions and inserting into DuckDB...")
    products_df = pd.read_csv(DIM_PRODUCTS_CSV)
    generate_transactions(signons_df, products_df, conn)


def main():
    parser = argparse.ArgumentParser(description="Data generation entrypoint control.")
    parser.add_argument(
        "--entrypoint",
        choices=["players", "products", "signons", "sessions", "transactions", "all"],
        default="all",
        help="Where to start in the data generation process."
    )
    args = parser.parse_args()

    print("📦 Connecting to DuckDB...")
    conn = connect_to_duckdb()

    player_ids = None
    signons_df = None
    country_map = None

    if args.entrypoint in ("players", "all"):
        player_ids = run_players(conn)

    if args.entrypoint in ("products", "all"):
        run_products()

    if args.entrypoint in ("signons", "all"):
        if player_ids is None:
            players_df = load_table_to_df(conn, "raw", "dim_players")
            if players_df is None or players_df.empty:
                print("⚠️ No players found in DB, generating default players.")
                player_ids = generate_player_ids(DEFAULT_STARTING_PLAYERS)
                df_players = pd.DataFrame({"playerId": player_ids})
                write_dataframe_to_table(conn, "raw", "dim_players", df_players, primary_key="playerId", replace=True)
            else:
                player_ids = players_df["playerId"].tolist()
        signons_df, country_map = run_signons(conn, player_ids)

    if args.entrypoint in ("sessions", "all"):
        if signons_df is None:
            signons_df = load_table_to_df(conn, "raw", "fact_signons")
            if signons_df is None:
                print("⚠️ No sign-ons found in DB, regenerating.")
                players_df = load_table_to_df(conn, "raw", "dim_players")
                if players_df is None or players_df.empty:
                    player_ids = generate_player_ids(DEFAULT_STARTING_PLAYERS)
                    df_players = pd.DataFrame({"playerId": player_ids})
                    write_dataframe_to_table(conn, "raw", "dim_players", df_players, primary_key="playerId", replace=True)
                else:
                    player_ids = players_df["playerId"].tolist()
                signons_df = model_sign_ons(player_ids, n_days=365)
                write_dataframe_to_table(conn, "raw", "fact_signons", signons_df, replace=True)
                country_map = assign_countries(player_ids)
            else:
                country_map = assign_countries(signons_df["playerId"].unique())
        run_sessions(conn, signons_df, country_map)

    if args.entrypoint in ("transactions", "all"):
        if signons_df is None:
            signons_df = load_table_to_df(conn, "raw", "fact_signons")
            if signons_df is None:
                print("⚠️ No sign-ons found in DB, regenerating.")
                players_df = load_table_to_df(conn, "raw", "dim_players")
                if players_df is None or players_df.empty:
                    player_ids = generate_player_ids(DEFAULT_STARTING_PLAYERS)
                    df_players = pd.DataFrame({"playerId": player_ids})
                    write_dataframe_to_table(conn, "raw", "dim_players", df_players, primary_key="playerId", replace=True)
                else:
                    player_ids = players_df["playerId"].tolist()
                signons_df = model_sign_ons(player_ids, n_days=365)
                write_dataframe_to_table(conn, "raw", "fact_signons", signons_df, replace=True)
        run_transactions(conn, signons_df)

    print("✅ Done!")


if __name__ == "__main__":
    main()

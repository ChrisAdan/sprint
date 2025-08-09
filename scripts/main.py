from utils import generate_player_ids, model_sign_ons, assign_countries
from session_generator import generate_sessions
from loader import connect_to_duckdb
from product_generator import generate_products
from transaction_generator import generate_transactions

def main():
    print("📦 Connecting to DuckDB...")
    conn = connect_to_duckdb()

    print("🛍 Generating product dimension seed file...")
    generate_products()  # idempotent overwrite

    print("👥 Generating player IDs...")
    try:
        n_players = int(input("Enter number of players to simulate: ").strip())
    except ValueError:
        print("❌ Invalid number entered.")
        return

    player_ids = generate_player_ids(n_players=n_players)

    print("📅 Modeling player sign-ons...")
    signins_df = model_sign_ons(player_ids, n_days=365)
    country_map = assign_countries(player_ids)

    print("🎮 Generating sessions and inserting into DuckDB...")
    generate_sessions(signins_df, country_map, conn)

    print("💸 Generating transactions and inserting into DuckDB...")
    # Read the product seed CSV so we can sample from it
    import pandas as pd
    products_df = pd.read_csv("dbt_project/seeds/dim_products.csv")

    generate_transactions(signins_df, products_df, conn)

    print("✅ Done! All data generated and stored.")

if __name__ == "__main__":
    main()

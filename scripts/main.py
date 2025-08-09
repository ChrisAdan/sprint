from utils import generate_player_ids, model_sign_ons, assign_countries
from session_generator import generate_sessions
from load_to_duckdb import connect_to_duckdb
from generate_products import generate_products  # NEW

def main():
    """
    Main entrypoint for the simulation pipeline.
    - Connects to DuckDB
    - Generates and saves dim_products.csv for dbt seeding
    - Prompts user for number of players
    - Generates player IDs and sign-on events
    - Assigns countries
    - Runs session simulation and inserts heartbeat + summary data into DuckDB
    """
    print("📦 Connecting to DuckDB...")
    conn = connect_to_duckdb()

    print("🛍 Generating product dimension seed file...")
    generate_products()  # idempotent CSV overwrite

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

    print("✅ Done! All sessions generated and stored.")


if __name__ == "__main__":
    main()

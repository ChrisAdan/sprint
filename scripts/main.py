from src.utils import generate_player_ids, model_sign_ons, assign_countries
from src.generate_sample_data import generate_sessions
from src.load_to_duckdb import connect_to_duckdb

def main():
    print("📦 Connecting to DuckDB...")
    conn = connect_to_duckdb()

    print("👥 Generating player IDs...")
    n_players = int(input("Enter number of players to simulate: ").strip())
    player_ids = generate_player_ids(n_players=n_players)

    print("📅 Modeling player sign-ons...")
    signins_df = model_sign_ons(player_ids, n_days=365)
    country_map = assign_countries(player_ids)

    print("🎮 Generating sessions and inserting into DuckDB...")
    generate_sessions(signins_df, country_map, conn)

    print("✅ Done!")

if __name__ == "__main__":
    main()

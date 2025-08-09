import uuid
import random
from pathlib import Path
from datetime import datetime, timedelta
from itertools import islice

import numpy as np
import pandas as pd

from loader import write_session_to_disk, stage_session
from utils import SESSION_PATH, SESSION_MAX_DURATION_SECONDS, \
    MIN_TEAMS, MAX_TEAMS, MIN_PLAYERS_PER_TEAM, MAX_PLAYERS_PER_TEAM, AVG_DAILY_SESSIONS

def chunked(iterable, size):
    it = iter(iterable)
    return iter(lambda: list(islice(it, size)), [])


def generate_sessions(
    signins_df,
    country_map,
    duck_conn,
    average_sessions_per_day=AVG_DAILY_SESSIONS,
    session_dir: Path = SESSION_PATH,
):
    """
    Idempotently generate sessions for each day and insert into DuckDB.

    Args:
        signins_df (pd.DataFrame): Columns: playerId, date
        country_map (dict): playerId -> country
        duck_conn (duckdb.DuckDBPyConnection)
        average_sessions_per_day (float): Controls session density
        session_dir (Path): where to write session JSON files
    """
    session_dir.mkdir(parents=True, exist_ok=True)

    # Clear previous sessions for idempotency:
    duck_conn.execute("DROP TABLE IF EXISTS raw.event_session")
    duck_conn.execute("DROP TABLE IF EXISTS stage.fact_session")

    summaries = []
    players_by_day = signins_df.groupby("date")["playerId"].apply(list)

    for date, players_today in players_by_day.items():
        np.random.shuffle(players_today)
        player_pool = players_today.copy()

        estimated_sessions = int(
            (len(player_pool) * average_sessions_per_day) / (MAX_TEAMS * MAX_PLAYERS_PER_TEAM)
        )
        sessions_today = 0

        while player_pool and sessions_today < estimated_sessions:
            num_teams = np.random.randint(MIN_TEAMS, MAX_TEAMS + 1)
            players_per_team = np.random.randint(MIN_PLAYERS_PER_TEAM, MAX_PLAYERS_PER_TEAM + 1)
            total_players = num_teams * players_per_team

            if len(player_pool) < total_players:
                break

            group = [player_pool.pop() for _ in range(total_players)]
            team_ids = [str(uuid.uuid4()) for _ in range(num_teams)]
            teams = {
                team_ids[i]: group[i * players_per_team : (i + 1) * players_per_team]
                for i in range(num_teams)
            }

            session_id = str(uuid.uuid4())
            session_start = datetime.combine(pd.to_datetime(date), datetime.min.time()) + timedelta(
                seconds=random.randint(0, 60 * 60 * 12)
            )
            session_end = session_start + timedelta(seconds=SESSION_MAX_DURATION_SECONDS)

            speed_map = {pid: np.random.randint(1, 4) for pid in group}
            durations = {pid: np.random.randint(120, SESSION_MAX_DURATION_SECONDS + 1) for pid in group}

            heartbeat_data = simulate_heartbeats(
                player_ids=group,
                session_id=session_id,
                team_ids=list(teams.keys()),
                session_start=session_start,
                speed_map=speed_map,
                durations=durations,
            )

            total_kills = np.random.randint(10, 60)
            total_deaths = total_kills

            kill_dist = np.random.multinomial(total_kills, np.random.dirichlet(np.ones(len(group))))
            death_dist = np.random.multinomial(total_deaths, np.random.dirichlet(np.ones(len(group))))

            for i, pid in enumerate(group):
                summaries.append(
                    {
                        "playerId": pid,
                        "sessionId": session_id,
                        "eventDateTime": session_end.isoformat(),
                        "country": country_map[pid],
                        "eventLengthSeconds": durations[pid],
                        "kills": kill_dist[i],
                        "deaths": death_dist[i],
                    }
                )

            # Idempotent session JSON and DuckDB insert
            write_session_to_disk(
                session_id=session_id,
                session_start=session_start,
                session_end=session_end,
                heartbeat_data=heartbeat_data,
                duck_conn=duck_conn,
                session_dir=session_dir,
            )

            sessions_today += 1

    summary_df = pd.DataFrame(summaries)

    # Idempotent overwrite stage table
    stage_session(summary_df, duck_conn)

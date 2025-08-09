import uuid
import random
from pathlib import Path
from datetime import datetime, timedelta
from itertools import islice

import numpy as np
import pandas as pd

from heartbeat_generator import simulate_heartbeats, STEP_FUNCTIONS
from loader import write_json_record_to_duckdb, clear_old_data, write_dataframe_to_table
from utils import (
    SESSION_PATH,
    SESSION_MAX_DURATION_SECONDS,
    MIN_TEAMS,
    MAX_TEAMS,
    MIN_PLAYERS_PER_TEAM,
    MAX_PLAYERS_PER_TEAM,
    AVG_DAILY_SESSIONS,
)


def chunked(iterable, size):
    it = iter(iterable)
    return iter(lambda: list(islice(it, size)), [])

def clear_sessions(duck_conn):
    """Clear session and related tables for idempotency."""
    clear_old_data(duck_conn, level="sessions")

def get_players_grouped_by_day(signins_df):
    """Group players by their sign-in day."""
    return signins_df.groupby("date")["playerId"].apply(list)

def estimate_sessions_per_day(num_players, average_sessions_per_day):
    """Estimate how many sessions to generate per day."""
    max_team_size = MAX_TEAMS * MAX_PLAYERS_PER_TEAM
    return int((num_players * average_sessions_per_day) / max_team_size)

def generate_team_structure(player_pool):
    """Randomly create teams with players from the pool."""
    num_teams = np.random.randint(MIN_TEAMS, MAX_TEAMS + 1)
    players_per_team = np.random.randint(MIN_PLAYERS_PER_TEAM, MAX_PLAYERS_PER_TEAM + 1)
    total_players = num_teams * players_per_team

    if len(player_pool) < total_players:
        return None, None, None

    group = [player_pool.pop() for _ in range(total_players)]
    team_ids = [str(uuid.uuid4()) for _ in range(num_teams)]
    teams = {
        team_ids[i]: group[i * players_per_team : (i + 1) * players_per_team]
        for i in range(num_teams)
    }

    return group, team_ids, teams

def generate_session_times(date):
    """Generate randomized session start and end times."""
    session_start = datetime.combine(pd.to_datetime(date), datetime.min.time()) + timedelta(
        seconds=random.randint(0, 60 * 60 * 12)
    )
    session_end = session_start + timedelta(seconds=SESSION_MAX_DURATION_SECONDS)
    return session_start, session_end

def assign_behavior_and_speed(group):
    """Assign movement behaviors and speeds per player."""
    behavior_types = list(STEP_FUNCTIONS.keys())
    behavior_map = {pid: random.choice(behavior_types) for pid in group}
    speed_map = {pid: np.random.randint(1, 4) for pid in group}
    durations = {pid: np.random.randint(120, SESSION_MAX_DURATION_SECONDS + 1) for pid in group}
    return behavior_map, speed_map, durations

def generate_kill_death_distribution(group):
    """Randomly generate kills and deaths distributions for players."""
    total_kills = np.random.randint(10, 60)
    total_deaths = total_kills
    kill_dist = np.random.multinomial(total_kills, np.random.dirichlet(np.ones(len(group))))
    death_dist = np.random.multinomial(total_deaths, np.random.dirichlet(np.ones(len(group))))
    return kill_dist, death_dist

def write_session_to_disk(
    session_id: str,
    session_start: datetime,
    session_end: datetime,
    heartbeat_data: list,
    duck_conn,
    session_dir: Path,
):
    """
    Write session heartbeat data to disk as JSON and optionally insert raw JSON into DuckDB.
    Now uses the updated write_json_record API with write_to_db=True.
    """
    if session_id is None:
        print("⚠️ No session_id provided, skipping write_session_to_disk.")
        return

    session_json = {
        "sessionId": session_id,
        "startTime": session_start.isoformat(),
        "endTime": session_end.isoformat(),
        "heartbeats": heartbeat_data,
    }

    write_json_record_to_duckdb(
        duck_conn=duck_conn,
        schema="raw",
        table="event_session",
        record_id_col="sessionId",
        record_id=session_id,
        json_obj=session_json,
        directory=session_dir,
        created_at_col="createdAt",
        write_to_db=True,  # enable insert into DuckDB
    )

def save_session_summaries(summary_df, duck_conn):
    """
    Save the session summary DataFrame to DuckDB 'stage.fact_session' table.
    """
    write_dataframe_to_table(
        duck_conn=duck_conn,
        schema="stage",
        table="fact_session",
        df=summary_df,
        primary_key=None,  # set if you have a natural key, else None
        replace=True,
    )

def generate_sessions(
    signins_df,
    country_map,
    duck_conn,
    average_sessions_per_day=AVG_DAILY_SESSIONS,
    session_dir: Path = SESSION_PATH,
):
    """Main orchestration function to generate sessions and write data."""
    session_dir.mkdir(parents=True, exist_ok=True)

    clear_sessions(duck_conn)

    summaries = []
    players_by_day = get_players_grouped_by_day(signins_df)

    for date, players_today in players_by_day.items():
        print(f'Generating sessions for {date}: {len(players_today)} players to simulate.')
        np.random.shuffle(players_today)
        player_pool = players_today.copy()

        estimated_sessions = estimate_sessions_per_day(len(player_pool), average_sessions_per_day)
        sessions_today = 0

        while player_pool and sessions_today < estimated_sessions:
            group, _, teams = generate_team_structure(player_pool)
            if group is None:
                break

            session_start, session_end = generate_session_times(date)
            behavior_map, speed_map, durations = assign_behavior_and_speed(group)

            # Flatten player to team_id mapping
            player_to_team = {pid: tid for tid, players in teams.items() for pid in players}

            heartbeat_data = simulate_heartbeats(
                player_ids=group,
                session_id=str(uuid.uuid4()),
                team_ids=player_to_team,
                session_start=session_start,
                speed_map=speed_map,
                durations=durations,
                behavior_map=behavior_map,
            )

            kill_dist, death_dist = generate_kill_death_distribution(group)

            for i, pid in enumerate(group):
                summaries.append(
                    {
                        "playerId": pid,
                        "sessionId": heartbeat_data[0]["sessionId"] if heartbeat_data else None,
                        "eventDateTime": session_end.isoformat(),
                        "country": country_map.get(pid, "Unknown"),
                        "eventLengthSeconds": durations[pid],
                        "kills": kill_dist[i],
                        "deaths": death_dist[i],
                    }
                )

            write_session_to_disk(
                session_id=heartbeat_data[0]["sessionId"] if heartbeat_data else None,
                session_start=session_start,
                session_end=session_end,
                heartbeat_data=heartbeat_data,
                duck_conn=duck_conn,
                session_dir=session_dir,
            )

            sessions_today += 1

    summary_df = pd.DataFrame(summaries)
    save_session_summaries(summary_df, duck_conn)

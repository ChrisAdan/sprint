import json
from datetime import datetime
from pathlib import Path
import duckdb

from utils import SESSION_PATH

DB_PATH = Path("data/synthetic.duckdb")


def connect_to_duckdb():
    """
    Connects to the DuckDB database file at data/synthetic.duckdb, creating directories if needed.
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(DB_PATH)


def write_session_to_disk(session_id, session_start, session_end, heartbeat_data, duck_conn):
    """
    Writes a full session JSON payload to disk and inserts into DuckDB raw.event_session table.
    """
    json_obj = {
        "session_id": session_id,
        "start_time": session_start.isoformat(),
        "end_time": session_end.isoformat(),
        "heartbeats": heartbeat_data
    }

    json_str = json.dumps(json_obj, indent=2)
    created_at = datetime.now(datetime.timezone.utc).isoformat()
    json_path = SESSION_PATH / f"{session_id}_{session_end.isoformat()}.json"
    json_path.write_text(json_str)

    duck_conn.execute(
        "INSERT INTO raw.event_session (recordId, rawResponse, createdAt) VALUES (?, ?, ?)",
        (session_id, json_str, created_at)
    )


def stage_session(summary_df, duck_conn):
    """
    Writes session summaries to stage.fact_session in DuckDB.
    """
    duck_conn.execute("CREATE TABLE IF NOT EXISTS stage.fact_session AS SELECT * FROM summary_df LIMIT 0")
    duck_conn.register("summary_df", summary_df)
    duck_conn.execute("INSERT INTO stage.fact_session SELECT * FROM summary_df")

import json
import uuid
import shutil
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

# Debug sys.path to ensure PYTHONPATH is correct (esp. for direnv)
import sys
import pprint
print("PYTHONPATH from runtime sys.path:")
pprint.pprint(sys.path)

# ✅ Imports from src/
from load_to_duckdb import connect_to_duckdb, write_session_to_disk, stage_session
import utils


@pytest.fixture(scope="session")
def session_path():
    """
    Creates a temporary directory for session JSON files,
    overrides utils.SESSION_PATH for test isolation,
    and cleans up after all tests in the session.
    """
    temp_dir = Path(tempfile.mkdtemp())
    utils.SESSION_PATH = temp_dir
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture(scope="module")
def duck_conn(session_path):
    """
    Provides a DuckDB connection for testing, ensuring schemas and tables are created.
    Depends on session_path fixture to ensure session path setup.
    """
    conn = connect_to_duckdb()
    # Clean and recreate test tables
    conn.execute("DROP TABLE IF EXISTS raw.event_session")
    conn.execute("DROP TABLE IF EXISTS stage.fact_session")
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    conn.execute("CREATE SCHEMA IF NOT EXISTS stage")
    conn.execute("""
        CREATE TABLE raw.event_session (
            recordId TEXT,
            rawResponse TEXT,
            createdAt TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE stage.fact_session (
            playerId TEXT,
            sessionId TEXT,
            eventDateTime TIMESTAMP,
            country TEXT,
            eventLengthSeconds INTEGER,
            kills INTEGER,
            deaths INTEGER
        )
    """)
    yield conn
    conn.close()


def test_write_session_to_disk_and_duckdb(duck_conn, session_path):
    """
    Tests writing a game session JSON payload to disk and insertion into DuckDB raw.event_session table.

    Verifies:
    - JSON file created in session_path
    - JSON structure uses camelCase keys (sessionId, playerId, teamId, positionX/Y/Z)
    - DuckDB row exists with matching sessionId and correct JSON data
    """
    session_id = str(uuid.uuid4())
    session_start = datetime.now(timezone.utc)
    session_end = session_start + timedelta(minutes=30)

    heartbeat_data = [
        {
            "playerId": "abc123",
            "sessionId": session_id,
            "teamId": "redTeam",
            "timestamp": session_start.isoformat(),
            "positionX": 1.1,
            "positionY": 2.2,
            "positionZ": 3.3
        }
    ]

    # ✅ Write to disk and DuckDB
    write_session_to_disk(
        session_id=session_id,
        session_start=session_start,
        session_end=session_end,
        heartbeat_data=heartbeat_data,
        duck_conn=duck_conn,
        session_dir=session_path
    )

    # ✅ Check JSON file creation
    files = list(session_path.glob(f"{session_id}_*.json"))
    assert len(files) == 1, "Expected exactly one JSON file for the session"
    json_file = files[0]
    assert json_file.exists()

    with open(json_file, "r") as f:
        payload = json.load(f)

    assert "sessionId" in payload
    assert payload["sessionId"] == session_id
    assert "heartbeats" in payload
    hb = payload["heartbeats"][0]
    assert hb["playerId"] == "abc123"
    assert hb["sessionId"] == session_id
    assert hb["teamId"] == "redTeam"
    assert hb["positionX"] == 1.1
    assert hb["positionY"] == 2.2
    assert hb["positionZ"] == 3.3

    # ✅ Verify DuckDB insertion
    row = duck_conn.execute(
        "SELECT * FROM raw.event_session WHERE recordId = ?", (session_id,)
    ).fetchone()

    assert row is not None
    assert row[0] == session_id
    stored_json = json.loads(row[1])
    assert stored_json["sessionId"] == session_id


def test_stage_session(duck_conn):
    """
    Tests inserting a session summary DataFrame into DuckDB stage.fact_session table.

    Verifies:
    - Table created and data inserted correctly
    - Data matches input camelCase keys and values
    """
    df = pd.DataFrame([
        {
            "playerId": "player001",
            "sessionId": "sess001",
            "eventDateTime": datetime.now(timezone.utc).isoformat(),
            "country": "US",
            "eventLengthSeconds": 1800,
            "kills": 7,
            "deaths": 5
        }
    ])

    stage_session(df, duck_conn)

    rows = duck_conn.execute("SELECT * FROM stage.fact_session").fetchall()
    assert len(rows) == 1
    row = rows[0]

    assert row[0] == "player001"
    assert row[1] == "sess001"
    assert row[3] == "US"
    assert row[4] == 1800
    assert row[5] == 7
    assert row[6] == 5

import json
from datetime import datetime, timezone
from pathlib import Path
import duckdb
import numpy as np
import pandas as pd

DB_PATH = Path("data/synthetic.duckdb")

def connect_to_duckdb():
    """
    Connects to the DuckDB database file at data/synthetic.duckdb, creating directories if needed.
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(DB_PATH)


def ensure_table_exists(
    duck_conn: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
    columns_def: str,
):
    """
    Ensures the given schema and table exist in DuckDB.

    Args:
        duck_conn: Active DuckDB connection.
        schema: Schema name as string (e.g. 'raw').
        table: Table name as string (e.g. 'event_session').
        columns_def: Columns and types SQL string for CREATE TABLE.
    """
    duck_conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    duck_conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            {columns_def}
        )
    """)


def write_json_to_disk_and_duckdb(
    table_name: str,
    record_id_col: str,
    record_id: str,
    json_obj: dict,
    duck_conn: duckdb.DuckDBPyConnection,
    directory: Path,
    created_at_col: str = "createdAt",
):
    """
    Generic helper to write a JSON record to disk and insert into DuckDB.

    Args:
        table_name (str): DuckDB table to insert into, e.g. 'raw.event_session'.
        record_id_col (str): Name of the primary key column.
        record_id (str): Primary key value for the record.
        json_obj (dict): JSON-serializable dictionary to save and insert.
        duck_conn (duckdb.DuckDBPyConnection): Active DuckDB connection.
        directory (Path): Directory to write JSON files.
        created_at_col (str): Column name for insertion timestamp.
    """
    directory.mkdir(parents=True, exist_ok=True)

    # Parse schema and table
    schema, table = table_name.split(".", 1)

    # Define expected columns for this generic JSON table
    columns_def = f"""
        {record_id_col} VARCHAR PRIMARY KEY,
        rawResponse VARCHAR,
        {created_at_col} TIMESTAMP
    """

    ensure_table_exists(duck_conn, schema, table, columns_def)

    json_str = json.dumps(json_obj, indent=2)
    created_at = datetime.now(timezone.utc).isoformat()

    filename = f"{record_id}_{json_obj.get('endTime', datetime.now(timezone.utc).isoformat())}.json"
    json_path = directory / filename
    json_path.write_text(json_str)

    duck_conn.execute(
        f"INSERT INTO {table_name} ({record_id_col}, rawResponse, {created_at_col}) VALUES (?, ?, ?)",
        (record_id, json_str, created_at)
    )


def write_session_to_disk(session_id, session_start, session_end, heartbeat_data, duck_conn, session_dir: Path):
    """
    Writes a full session JSON payload to disk and inserts into DuckDB raw.event_session table.
    """
    json_obj = {
        "sessionId": session_id,
        "startTime": session_start.isoformat(),
        "endTime": session_end.isoformat(),
        "heartbeats": heartbeat_data,
    }
    write_json_to_disk_and_duckdb(
        table_name="raw.event_session",
        record_id_col="recordId",
        record_id=session_id,
        json_obj=json_obj,
        duck_conn=duck_conn,
        directory=session_dir,
        created_at_col="createdAt",
    )


def stage_summary_sessions(summary_df, duck_conn):
    """
    Writes session summaries to stage.fact_session in DuckDB.
    """

    # Create the table with schema matching summary_df columns
    cols = ", ".join(
        f"{col} VARCHAR" if summary_df[col].dtype == object else
        f"{col} INTEGER" if np.issubdtype(summary_df[col].dtype, np.integer) else
        f"{col} DOUBLE" if np.issubdtype(summary_df[col].dtype, np.floating) else
        f"{col} VARCHAR"
        for col in summary_df.columns
    )
    ensure_table_exists(duck_conn, "stage", "fact_session", cols)

    duck_conn.register("summary_df", summary_df)
    duck_conn.execute("INSERT INTO stage.fact_session SELECT * FROM summary_df")


def save_transactions_json(date, transactions, out_dir=None):
    """
    Saves a list of transaction records to a JSON file on disk for a given date.
    """
    transactions_dir = Path("data/transactions") if out_dir is None else Path(out_dir)
    transactions_dir.mkdir(parents=True, exist_ok=True)

    def convert(obj):
        if isinstance(obj, dict):
            return {k: convert(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert(i) for i in obj]
        elif isinstance(obj, (np.bool_, np.integer, np.floating)):
            return obj.item()
        else:
            return obj

    clean_transactions = convert(transactions)

    date_str = date.strftime("%Y%m%d")
    out_path = transactions_dir / f"transactions_{date_str}.json"
    json_str = json.dumps(clean_transactions, indent=2)
    out_path.write_text(json_str)
    print(f"💾 Saved {len(transactions)} transactions to {out_path}")


def write_transactions_to_duckdb(transactions, duck_conn):
    """
    Inserts transactions into DuckDB raw.event_transactions table.
    """
    if not transactions:
        return

    # # Prepare schema/table
    # duck_conn.execute("CREATE SCHEMA IF NOT EXISTS raw")

    df = pd.DataFrame(transactions)

    # Normalize booleans and numpy types to Python native for safety
    df = df.convert_dtypes().astype(object)

    # Define columns based on df dtypes (simplified all as VARCHAR here)
    cols = ", ".join(f"{col} VARCHAR" for col in df.columns)

    ensure_table_exists(duck_conn, "raw", "event_transactions", cols)

    duck_conn.register("transactions_df", df)
    duck_conn.execute("INSERT INTO raw.event_transactions SELECT * FROM transactions_df")
    print(f"📥 Inserted {len(df)} transactions into raw.event_transactions")

"""
ETL pipeline orchestrator.

Runs Extract → Transform → Load in sequence and returns a summary dict.
"""

import sqlite3

from etl.extract import extract
from etl.load import create_tables, load
from etl.transform import transform


def run_pipeline(
    db_path: str = "flights.db",
    window_seconds: int = 3600,
) -> dict:
    """Execute the full ETL pipeline.

    Parameters
    ----------
    db_path:
        Path to the SQLite database file.  Created if it does not exist.
    window_seconds:
        Time window (in seconds) passed to the Extract step.

    Returns
    -------
    dict
        Summary with keys ``raw_count``, ``transformed_count``, and
        ``loaded_count``.
    """
    print("=" * 50)
    print("Starting ETL pipeline")
    print("=" * 50)

    # --- Extract ---
    raw_data = extract(window_seconds=window_seconds)
    raw_count = sum(len(v) for v in raw_data.values())
    print(f"[pipeline] Total raw flights extracted: {raw_count}")

    # --- Transform ---
    flights = transform(raw_data)

    # --- Load ---
    conn = sqlite3.connect(db_path)
    try:
        create_tables(conn)
        loaded_count = load(conn, flights)
    finally:
        conn.close()

    summary = {
        "raw_count": raw_count,
        "transformed_count": len(flights),
        "loaded_count": loaded_count,
    }
    print("=" * 50)
    print(f"Pipeline complete: {summary}")
    print("=" * 50)
    return summary

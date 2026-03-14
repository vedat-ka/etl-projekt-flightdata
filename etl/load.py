"""
Load step of the ETL pipeline.

Stores transformed flight data in a local SQLite database.
Schema:
  airlines (id, icao_prefix, name)
  flights  (id, …, airline_id → airlines.id)
"""

import sqlite3
from typing import List, Optional

# ------------------------------------------------------------------
# DDL
# ------------------------------------------------------------------

_CREATE_AIRLINES = """
CREATE TABLE IF NOT EXISTS airlines (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    icao_prefix TEXT    UNIQUE NOT NULL,
    name        TEXT
);
"""

_CREATE_FLIGHTS = """
CREATE TABLE IF NOT EXISTS flights (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    icao24               TEXT    NOT NULL,
    callsign             TEXT,
    departure_airport    TEXT,
    arrival_airport_icao TEXT,
    arrival_airport_iata TEXT,
    arrival_timestamp    INTEGER,
    arrival_date         TEXT,
    arrival_time         TEXT,
    arrival_year         INTEGER,
    arrival_month        INTEGER,
    arrival_day          INTEGER,
    arrival_hour         INTEGER,
    arrival_minute       INTEGER,
    airline_id           INTEGER,
    FOREIGN KEY (airline_id) REFERENCES airlines(id)
);
"""

# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def create_tables(conn: sqlite3.Connection) -> None:
    """Create ``airlines`` and ``flights`` tables if they do not exist."""
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON")
    cursor.execute(_CREATE_AIRLINES)
    cursor.execute(_CREATE_FLIGHTS)
    conn.commit()


def upsert_airline(
    conn: sqlite3.Connection, icao_prefix: str, name: Optional[str]
) -> int:
    """Insert airline if not present, then return its primary key.

    Parameters
    ----------
    conn:
        Open SQLite connection.
    icao_prefix:
        3-letter ICAO airline prefix (unique key).
    name:
        Human-readable airline name (may be ``None``).

    Returns
    -------
    int
        Primary key of the airline row.
    """
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR IGNORE INTO airlines (icao_prefix, name) VALUES (?, ?)",
        (icao_prefix, name),
    )
    conn.commit()
    cursor.execute("SELECT id FROM airlines WHERE icao_prefix = ?", (icao_prefix,))
    row = cursor.fetchone()
    return row[0]


# ------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------


def load(conn: sqlite3.Connection, flights: List[dict]) -> int:
    """Load transformed flight records into the database.

    Each flight's airline is upserted into the ``airlines`` table first;
    the resulting foreign key is stored in the ``flights`` row.

    Parameters
    ----------
    conn:
        Open SQLite connection (tables must already exist).
    flights:
        Output of :func:`etl.transform.transform`.

    Returns
    -------
    int
        Number of rows inserted into ``flights``.
    """
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON")

    inserted = 0
    for flight in flights:
        airline_id: Optional[int] = None
        if flight.get("airline_prefix"):
            airline_id = upsert_airline(
                conn, flight["airline_prefix"], flight.get("airline_name")
            )

        cursor.execute(
            """
            INSERT INTO flights (
                icao24, callsign, departure_airport,
                arrival_airport_icao, arrival_airport_iata,
                arrival_timestamp, arrival_date, arrival_time,
                arrival_year, arrival_month, arrival_day,
                arrival_hour, arrival_minute,
                airline_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                flight["icao24"],
                flight["callsign"],
                flight["departure_airport"],
                flight["arrival_airport_icao"],
                flight["arrival_airport_iata"],
                flight["arrival_timestamp"],
                flight["arrival_date"],
                flight["arrival_time"],
                flight["arrival_year"],
                flight["arrival_month"],
                flight["arrival_day"],
                flight["arrival_hour"],
                flight["arrival_minute"],
                airline_id,
            ),
        )
        inserted += 1

    conn.commit()
    print(f"[load] {inserted} flights inserted into database")
    return inserted

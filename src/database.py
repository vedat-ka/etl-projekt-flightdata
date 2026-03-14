import logging
import sqlite3
import time
from pathlib import Path

import pandas as pd


class SQLiteManager:
    """Handle schema creation and SQLite load operations."""

    def __init__(self, db_path: Path, logger: logging.Logger):
        self.db_path = db_path
        self.logger = logger

    def _connect(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.db_path)
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    @staticmethod
    def _rows_to_dicts(rows: list[sqlite3.Row]) -> list[dict]:
        return [dict(row) for row in rows]

    @staticmethod
    def _safe_identifier(identifier: str) -> str:
        if not identifier.replace("_", "").isalnum():
            raise ValueError("Invalid SQL identifier.")
        return identifier

    def init_schema(self, connection: sqlite3.Connection) -> None:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS airlines (
                airline_id INTEGER PRIMARY KEY AUTOINCREMENT,
                iata_code TEXT NOT NULL DEFAULT '',
                icao_code TEXT NOT NULL DEFAULT '',
                name TEXT NOT NULL,
                UNIQUE(iata_code, icao_code, name)
            );

            CREATE TABLE IF NOT EXISTS flights (
                flight_id INTEGER PRIMARY KEY AUTOINCREMENT,
                destination_airport_iata TEXT NOT NULL,
                destination_airport_name TEXT NOT NULL,
                origin_airport_iata TEXT,
                origin_country TEXT NOT NULL,
                flight_number TEXT NOT NULL,
                airline_id INTEGER NOT NULL,
                arrival_time TEXT NOT NULL,
                arrival_year INTEGER NOT NULL,
                arrival_month INTEGER NOT NULL,
                arrival_day INTEGER NOT NULL,
                arrival_hour INTEGER NOT NULL,
                arrival_minute INTEGER NOT NULL,
                arrival_second INTEGER NOT NULL,
                status TEXT,
                source_record_key TEXT NOT NULL UNIQUE,
                FOREIGN KEY (airline_id) REFERENCES airlines(airline_id)
            );
            """
        )

    @staticmethod
    def _table_columns(connection: sqlite3.Connection, table_name: str) -> list[dict]:
        rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
        columns = []
        for row in rows:
            columns.append(
                {
                    "cid": row[0],
                    "name": row[1],
                    "type": row[2],
                    "notnull": bool(row[3]),
                    "default_value": row[4],
                    "primary_key": bool(row[5]),
                }
            )
        return columns

    def get_dashboard_snapshot(self, recent_limit: int = 25) -> dict:
        connection = self._connect()
        connection.row_factory = sqlite3.Row
        try:
            self.init_schema(connection)
            airline_count = connection.execute("SELECT COUNT(*) AS count FROM airlines").fetchone()["count"]
            flight_count = connection.execute("SELECT COUNT(*) AS count FROM flights").fetchone()["count"]

            flights_by_destination = self._rows_to_dicts(
                connection.execute(
                    """
                    SELECT destination_airport_iata, COUNT(*) AS count
                    FROM flights
                    GROUP BY destination_airport_iata
                    ORDER BY destination_airport_iata
                    """
                ).fetchall()
            )
            status_distribution = self._rows_to_dicts(
                connection.execute(
                    """
                    SELECT COALESCE(status, 'UNKNOWN') AS status, COUNT(*) AS count
                    FROM flights
                    GROUP BY COALESCE(status, 'UNKNOWN')
                    ORDER BY count DESC, status ASC
                    """
                ).fetchall()
            )
            top_origin_countries = self._rows_to_dicts(
                connection.execute(
                    """
                    SELECT origin_country, COUNT(*) AS count
                    FROM flights
                    GROUP BY origin_country
                    ORDER BY count DESC, origin_country ASC
                    LIMIT 10
                    """
                ).fetchall()
            )
            top_airlines = self._rows_to_dicts(
                connection.execute(
                    """
                    SELECT a.name AS airline_name, COUNT(*) AS count
                    FROM flights f
                    JOIN airlines a ON a.airline_id = f.airline_id
                    GROUP BY a.name
                    ORDER BY count DESC, a.name ASC
                    LIMIT 10
                    """
                ).fetchall()
            )
            recent_flights = self._rows_to_dicts(
                connection.execute(
                    """
                    SELECT
                        f.flight_number,
                        f.destination_airport_iata,
                        f.destination_airport_name,
                        f.origin_airport_iata,
                        f.origin_country,
                        a.name AS airline_name,
                        f.arrival_time,
                        COALESCE(f.status, 'UNKNOWN') AS status
                    FROM flights f
                    JOIN airlines a ON a.airline_id = f.airline_id
                    ORDER BY f.arrival_time DESC, f.flight_number ASC
                    LIMIT ?
                    """,
                    (max(1, recent_limit),),
                ).fetchall()
            )
            top_airlines_by_airport_rows = self._rows_to_dicts(
                connection.execute(
                    """
                    SELECT
                        f.destination_airport_iata,
                        a.name AS airline_name,
                        COUNT(*) AS count
                    FROM flights f
                    JOIN airlines a ON a.airline_id = f.airline_id
                    GROUP BY f.destination_airport_iata, a.name
                    ORDER BY f.destination_airport_iata ASC, count DESC, a.name ASC
                    """
                ).fetchall()
            )
            timeframe_airline_rows = self._rows_to_dicts(
                connection.execute(
                    """
                    SELECT
                        f.destination_airport_iata,
                        CASE
                            WHEN f.arrival_hour BETWEEN 0 AND 5 THEN 'night'
                            WHEN f.arrival_hour BETWEEN 6 AND 11 THEN 'morning'
                            WHEN f.arrival_hour BETWEEN 12 AND 17 THEN 'afternoon'
                            ELSE 'evening'
                        END AS timeframe,
                        a.name AS airline_name,
                        COUNT(*) AS count
                    FROM flights f
                    JOIN airlines a ON a.airline_id = f.airline_id
                    GROUP BY f.destination_airport_iata, timeframe, a.name
                    ORDER BY f.destination_airport_iata ASC, timeframe ASC, count DESC, a.name ASC
                    """
                ).fetchall()
            )
            arrival_activity = self._rows_to_dicts(
                connection.execute(
                    """
                    SELECT
                        destination_airport_iata,
                        printf('%02d:00', arrival_hour) AS bucket,
                        COUNT(*) AS count
                    FROM flights
                    GROUP BY destination_airport_iata, arrival_hour
                    ORDER BY destination_airport_iata ASC, arrival_hour ASC
                    """
                ).fetchall()
            )

            top_airlines_by_airport: dict[str, list[dict]] = {}
            for row in top_airlines_by_airport_rows:
                airport_iata = row["destination_airport_iata"]
                top_airlines_by_airport.setdefault(airport_iata, [])
                if len(top_airlines_by_airport[airport_iata]) < 10:
                    top_airlines_by_airport[airport_iata].append(
                        {
                            "airline_name": row["airline_name"],
                            "count": row["count"],
                        }
                    )

            airlines_by_airport_and_timeframe: dict[str, dict[str, list[dict]]] = {}
            for row in timeframe_airline_rows:
                airport_iata = row["destination_airport_iata"]
                timeframe = row["timeframe"]
                airport_group = airlines_by_airport_and_timeframe.setdefault(airport_iata, {})
                timeframe_group = airport_group.setdefault(timeframe, [])
                if len(timeframe_group) < 10:
                    timeframe_group.append(
                        {
                            "airline_name": row["airline_name"],
                            "count": row["count"],
                        }
                    )

            db_exists = self.db_path.exists()
            db_size_bytes = self.db_path.stat().st_size if db_exists else 0

            return {
                "db_path": str(self.db_path),
                "db_exists": db_exists,
                "db_size_bytes": db_size_bytes,
                "airline_count": airline_count,
                "flight_count": flight_count,
                "flights_by_destination": flights_by_destination,
                "status_distribution": status_distribution,
                "top_origin_countries": top_origin_countries,
                "top_airlines": top_airlines,
                "top_airlines_by_airport": top_airlines_by_airport,
                "airlines_by_airport_and_timeframe": airlines_by_airport_and_timeframe,
                "arrival_activity": arrival_activity,
                "recent_flights": recent_flights,
                "tables": {
                    "airlines": self._table_columns(connection, "airlines"),
                    "flights": self._table_columns(connection, "flights"),
                },
            }
        finally:
            connection.close()

    def get_explorer_snapshot(self, table_name: str, limit: int = 50) -> dict:
        connection = self._connect()
        connection.row_factory = sqlite3.Row
        try:
            self.init_schema(connection)
            safe_table = self._safe_identifier(table_name)
            table_names = {
                row["name"]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
                ).fetchall()
            }
            if safe_table not in table_names:
                raise ValueError(f"Unknown table: {table_name}")

            row_count = connection.execute(f"SELECT COUNT(*) AS count FROM {safe_table}").fetchone()["count"]
            preview_rows = self._rows_to_dicts(
                connection.execute(f"SELECT * FROM {safe_table} LIMIT ?", (max(1, limit),)).fetchall()
            )

            tables = []
            for current_table in sorted(table_names):
                count = connection.execute(f"SELECT COUNT(*) AS count FROM {current_table}").fetchone()["count"]
                tables.append(
                    {
                        "name": current_table,
                        "row_count": count,
                        "columns": self._table_columns(connection, current_table),
                    }
                )

            return {
                "selected_table": safe_table,
                "selected_table_row_count": row_count,
                "preview_rows": preview_rows,
                "tables": tables,
            }
        finally:
            connection.close()

    def run_read_only_query(self, sql: str, limit: int = 200) -> dict:
        normalized_sql = sql.strip()
        lowered_sql = normalized_sql.lower().lstrip()
        if not lowered_sql:
            raise ValueError("SQL query must not be empty.")
        if ";" in lowered_sql.rstrip(";"):
            raise ValueError("Only a single SQL statement is allowed.")
        allowed_prefixes = ("select", "with", "pragma")
        if not lowered_sql.startswith(allowed_prefixes):
            raise ValueError("Only read-only SELECT, WITH, or PRAGMA queries are allowed.")

        blocked_keywords = (
            "insert ",
            "update ",
            "delete ",
            "drop ",
            "alter ",
            "create ",
            "replace ",
            "attach ",
            "detach ",
            "vacuum",
            "reindex",
            "truncate ",
        )
        if any(keyword in lowered_sql for keyword in blocked_keywords):
            raise ValueError("Only read-only SQL is permitted.")

        connection = self._connect()
        connection.row_factory = sqlite3.Row
        try:
            self.init_schema(connection)
            started_at = time.perf_counter()
            cursor = connection.execute(normalized_sql.rstrip(";"))
            rows = cursor.fetchmany(max(1, limit))
            elapsed_ms = round((time.perf_counter() - started_at) * 1000, 2)
            columns = [description[0] for description in (cursor.description or [])]
            return {
                "columns": columns,
                "rows": self._rows_to_dicts(rows),
                "row_count": len(rows),
                "execution_ms": elapsed_ms,
                "truncated": len(rows) == limit,
            }
        finally:
            connection.close()

    def _replace_snapshot(self, connection: sqlite3.Connection) -> None:
        """Replace the complete SQLite snapshot on each ETL run."""
        connection.execute("DELETE FROM flights")
        connection.execute("DELETE FROM airlines")
        connection.execute(
            "DELETE FROM sqlite_sequence WHERE name IN ('flights', 'airlines')"
        )
        self.logger.info("Cleared existing SQLite snapshot before load.")

    def load_flights(self, df: pd.DataFrame) -> dict:
        """Load transformed rows into SQLite and return summary statistics."""
        connection = self._connect()
        try:
            self.init_schema(connection)
            self._replace_snapshot(connection)

            if df.empty:
                self.logger.warning("No transformed flights available for SQLite load.")
                airline_count = connection.execute("SELECT COUNT(*) FROM airlines").fetchone()[0]
                flight_count = connection.execute("SELECT COUNT(*) FROM flights").fetchone()[0]
                connection.commit()
                return {
                    "db_path": str(self.db_path),
                    "airline_count": airline_count,
                    "flight_count": flight_count,
                }

            airline_rows = (
                df[["airline_iata", "airline_icao", "airline_name"]]
                .fillna("")
                .drop_duplicates()
                .itertuples(index=False, name=None)
            )
            connection.executemany(
                """
                INSERT OR IGNORE INTO airlines (iata_code, icao_code, name)
                VALUES (?, ?, ?)
                """,
                airline_rows,
            )

            airline_map = {
                (iata_code, icao_code, name): airline_id
                for airline_id, iata_code, icao_code, name in connection.execute(
                    "SELECT airline_id, iata_code, icao_code, name FROM airlines"
                )
            }

            flight_rows = []
            for row in df.fillna("").itertuples(index=False):
                airline_key = (row.airline_iata, row.airline_icao, row.airline_name)
                airline_id = airline_map[airline_key]
                arrival_ts = pd.Timestamp(row.arrival_time)
                flight_rows.append(
                    (
                        row.destination_airport_iata,
                        row.destination_airport_name,
                        row.origin_airport_iata,
                        row.origin_country,
                        row.flight_number,
                        airline_id,
                        arrival_ts.isoformat(),
                        int(row.arrival_year),
                        int(row.arrival_month),
                        int(row.arrival_day),
                        int(row.arrival_hour),
                        int(row.arrival_minute),
                        int(row.arrival_second),
                        row.status,
                        row.source_record_key,
                    )
                )

            connection.executemany(
                """
                INSERT INTO flights (
                    destination_airport_iata,
                    destination_airport_name,
                    origin_airport_iata,
                    origin_country,
                    flight_number,
                    airline_id,
                    arrival_time,
                    arrival_year,
                    arrival_month,
                    arrival_day,
                    arrival_hour,
                    arrival_minute,
                    arrival_second,
                    status,
                    source_record_key
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_record_key) DO UPDATE SET
                    destination_airport_iata = excluded.destination_airport_iata,
                    destination_airport_name = excluded.destination_airport_name,
                    origin_airport_iata = excluded.origin_airport_iata,
                    origin_country = excluded.origin_country,
                    flight_number = excluded.flight_number,
                    airline_id = excluded.airline_id,
                    arrival_time = excluded.arrival_time,
                    arrival_year = excluded.arrival_year,
                    arrival_month = excluded.arrival_month,
                    arrival_day = excluded.arrival_day,
                    arrival_hour = excluded.arrival_hour,
                    arrival_minute = excluded.arrival_minute,
                    arrival_second = excluded.arrival_second,
                    status = excluded.status
                """,
                flight_rows,
            )

            airline_count = connection.execute("SELECT COUNT(*) FROM airlines").fetchone()[0]
            flight_count = connection.execute("SELECT COUNT(*) FROM flights").fetchone()[0]
            connection.commit()
            self.logger.info(
                "SQLite load completed: %s airlines, %s flights",
                airline_count,
                flight_count,
            )
            return {
                "db_path": str(self.db_path),
                "airline_count": airline_count,
                "flight_count": flight_count,
            }
        except sqlite3.DatabaseError:
            connection.rollback()
            self.logger.exception("SQLite load failed.")
            raise
        finally:
            connection.close()
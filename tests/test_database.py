import logging
import sqlite3
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from src.database import SQLiteManager


def build_test_dataframe(records: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(records)
    df["arrival_time"] = pd.to_datetime(df["arrival_time"], utc=True)
    return df


class SQLiteManagerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "flights.sqlite"
        self.logger = logging.getLogger("test_sqlite_manager")
        self.manager = SQLiteManager(self.db_path, self.logger)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _sample_df(self) -> pd.DataFrame:
        return build_test_dataframe(
            [
                {
                    "destination_airport_iata": "BER",
                    "destination_airport_name": "Berlin Brandenburg",
                    "origin_airport_iata": "LHR",
                    "origin_country": "gb",
                    "flight_number": "BA 849",
                    "airline_iata": "BA",
                    "airline_icao": "BAW",
                    "airline_name": "British Airways",
                    "arrival_time": "2026-03-11T10:15:00Z",
                    "arrival_year": 2026,
                    "arrival_month": 3,
                    "arrival_day": 11,
                    "arrival_hour": 10,
                    "arrival_minute": 15,
                    "arrival_second": 0,
                    "status": "Expected",
                    "source_record_key": "BER|BA 849|BA|LHR|2026-03-11T10:15:00Z",
                },
                {
                    "destination_airport_iata": "CDG",
                    "destination_airport_name": "Paris Charles de Gaulle",
                    "origin_airport_iata": "FCO",
                    "origin_country": "it",
                    "flight_number": "AF 1305",
                    "airline_iata": "AF",
                    "airline_icao": "AFR",
                    "airline_name": "Air France",
                    "arrival_time": "2026-03-11T11:45:00Z",
                    "arrival_year": 2026,
                    "arrival_month": 3,
                    "arrival_day": 11,
                    "arrival_hour": 11,
                    "arrival_minute": 45,
                    "arrival_second": 0,
                    "status": "Expected",
                    "source_record_key": "CDG|AF 1305|AF|FCO|2026-03-11T11:45:00Z",
                },
            ]
        )

    def test_load_creates_foreign_key_relation(self) -> None:
        self.manager.load_flights(self._sample_df())

        connection = sqlite3.connect(self.db_path)
        try:
            foreign_keys = connection.execute("PRAGMA foreign_key_list(flights)").fetchall()
            self.assertEqual(len(foreign_keys), 1)
            self.assertEqual(foreign_keys[0][2], "airlines")
        finally:
            connection.close()

    def test_source_record_key_is_unique(self) -> None:
        self.manager.load_flights(self._sample_df())

        connection = sqlite3.connect(self.db_path)
        try:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
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
                    SELECT
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
                    FROM flights
                    LIMIT 1
                    """
                )
        finally:
            connection.close()

    def test_load_replaces_previous_snapshot(self) -> None:
        first_df = self._sample_df()
        second_df = build_test_dataframe(
            [
                {
                    "destination_airport_iata": "STR",
                    "destination_airport_name": "Stuttgart",
                    "origin_airport_iata": "MAD",
                    "origin_country": "es",
                    "flight_number": "EW 2881",
                    "airline_iata": "EW",
                    "airline_icao": "EWG",
                    "airline_name": "Eurowings",
                    "arrival_time": "2026-03-11T12:30:00Z",
                    "arrival_year": 2026,
                    "arrival_month": 3,
                    "arrival_day": 11,
                    "arrival_hour": 12,
                    "arrival_minute": 30,
                    "arrival_second": 0,
                    "status": "Expected",
                    "source_record_key": "STR|EW 2881|EW|MAD|2026-03-11T12:30:00Z",
                }
            ]
        )

        self.manager.load_flights(first_df)
        self.manager.load_flights(second_df)

        connection = sqlite3.connect(self.db_path)
        try:
            airline_count = connection.execute("SELECT COUNT(*) FROM airlines").fetchone()[0]
            flight_count = connection.execute("SELECT COUNT(*) FROM flights").fetchone()[0]
            destinations = connection.execute(
                "SELECT destination_airport_iata FROM flights ORDER BY destination_airport_iata"
            ).fetchall()
            self.assertEqual(airline_count, 1)
            self.assertEqual(flight_count, 1)
            self.assertEqual(destinations, [("STR",)])
        finally:
            connection.close()


if __name__ == "__main__":
    unittest.main()
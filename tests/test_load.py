"""Unit tests for etl.load."""

import sqlite3

import pytest

from etl.load import create_tables, load, upsert_airline


@pytest.fixture
def conn():
    """Provide an in-memory SQLite connection with tables already created."""
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    create_tables(connection)
    yield connection
    connection.close()


# ---------------------------------------------------------------------------
# create_tables
# ---------------------------------------------------------------------------


class TestCreateTables:
    def test_airlines_table_exists(self, conn):
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='airlines'"
        )
        assert cur.fetchone() is not None

    def test_flights_table_exists(self, conn):
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='flights'"
        )
        assert cur.fetchone() is not None

    def test_idempotent(self, conn):
        # Calling create_tables twice must not raise
        create_tables(conn)

    def test_flights_has_foreign_key_to_airlines(self, conn):
        cur = conn.execute("PRAGMA foreign_key_list(flights)")
        fk_rows = cur.fetchall()
        tables = [row["table"] for row in fk_rows]
        assert "airlines" in tables


# ---------------------------------------------------------------------------
# upsert_airline
# ---------------------------------------------------------------------------


class TestUpsertAirline:
    def test_inserts_new_airline(self, conn):
        airline_id = upsert_airline(conn, "DLH", "Lufthansa")
        assert isinstance(airline_id, int)
        cur = conn.execute("SELECT * FROM airlines WHERE icao_prefix='DLH'")
        row = cur.fetchone()
        assert row is not None
        assert row["name"] == "Lufthansa"

    def test_idempotent_on_duplicate(self, conn):
        id1 = upsert_airline(conn, "DLH", "Lufthansa")
        id2 = upsert_airline(conn, "DLH", "Lufthansa")
        assert id1 == id2
        cur = conn.execute("SELECT COUNT(*) FROM airlines WHERE icao_prefix='DLH'")
        assert cur.fetchone()[0] == 1

    def test_accepts_none_name(self, conn):
        airline_id = upsert_airline(conn, "XYZ", None)
        cur = conn.execute("SELECT name FROM airlines WHERE id=?", (airline_id,))
        assert cur.fetchone()["name"] is None


# ---------------------------------------------------------------------------
# load
# ---------------------------------------------------------------------------

SAMPLE_FLIGHT = {
    "icao24": "3c6444",
    "callsign": "DLH001",
    "departure_airport": "LFPG",
    "arrival_airport_icao": "EDDB",
    "arrival_airport_iata": "BER",
    "arrival_timestamp": 1700004600,
    "arrival_date": "2023-11-14",
    "arrival_time": "22:30:00",
    "arrival_year": 2023,
    "arrival_month": 11,
    "arrival_day": 14,
    "arrival_hour": 22,
    "arrival_minute": 30,
    "airline_prefix": "DLH",
    "airline_name": "Lufthansa",
}


class TestLoad:
    def test_inserts_flights(self, conn):
        count = load(conn, [SAMPLE_FLIGHT])
        assert count == 1
        cur = conn.execute("SELECT * FROM flights")
        rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0]["callsign"] == "DLH001"

    def test_sets_airline_id_foreign_key(self, conn):
        load(conn, [SAMPLE_FLIGHT])
        cur = conn.execute("SELECT airline_id FROM flights")
        airline_id = cur.fetchone()["airline_id"]
        assert airline_id is not None
        cur2 = conn.execute("SELECT icao_prefix FROM airlines WHERE id=?", (airline_id,))
        assert cur2.fetchone()["icao_prefix"] == "DLH"

    def test_flight_without_airline_prefix(self, conn):
        flight = dict(SAMPLE_FLIGHT, airline_prefix=None, airline_name=None)
        count = load(conn, [flight])
        assert count == 1
        cur = conn.execute("SELECT airline_id FROM flights")
        assert cur.fetchone()["airline_id"] is None

    def test_multiple_flights_same_airline(self, conn):
        flight2 = dict(SAMPLE_FLIGHT, icao24="aabbcc", callsign="DLH002")
        count = load(conn, [SAMPLE_FLIGHT, flight2])
        assert count == 2
        # Airline should only be inserted once
        cur = conn.execute("SELECT COUNT(*) FROM airlines WHERE icao_prefix='DLH'")
        assert cur.fetchone()[0] == 1

    def test_returns_zero_for_empty_list(self, conn):
        assert load(conn, []) == 0

    def test_all_fields_stored(self, conn):
        load(conn, [SAMPLE_FLIGHT])
        cur = conn.execute("SELECT * FROM flights")
        row = cur.fetchone()
        assert row["icao24"] == "3c6444"
        assert row["departure_airport"] == "LFPG"
        assert row["arrival_airport_icao"] == "EDDB"
        assert row["arrival_airport_iata"] == "BER"
        assert row["arrival_timestamp"] == 1700004600
        assert row["arrival_date"] == "2023-11-14"
        assert row["arrival_time"] == "22:30:00"
        assert row["arrival_year"] == 2023
        assert row["arrival_month"] == 11
        assert row["arrival_day"] == 14
        assert row["arrival_hour"] == 22
        assert row["arrival_minute"] == 30

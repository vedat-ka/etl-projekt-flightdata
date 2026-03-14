"""Unit tests for etl.transform."""

import pytest

from etl.transform import (
    AIRLINE_NAMES,
    decompose_arrival_time,
    extract_airline_prefix,
    transform,
    transform_flight,
)

# ---------------------------------------------------------------------------
# Sample raw data
# ---------------------------------------------------------------------------

VALID_RAW = {
    "icao24": "3C6444",
    "firstSeen": 1700001000,
    "estDepartureAirport": "LFPG",
    "lastSeen": 1700004600,
    "estArrivalAirport": "EDDB",
    "callsign": "DLH001 ",
    "estDepartureAirportHorizDistance": 1234,
    "estDepartureAirportVertDistance": 100,
    "estArrivalAirportHorizDistance": 500,
    "estArrivalAirportVertDistance": 50,
    "departureAirportCandidatesCount": 0,
    "arrivalAirportCandidatesCount": 0,
}


# ---------------------------------------------------------------------------
# extract_airline_prefix
# ---------------------------------------------------------------------------


class TestExtractAirlinePrefix:
    def test_extracts_three_letter_prefix(self):
        assert extract_airline_prefix("DLH001") == "DLH"

    def test_strips_whitespace(self):
        assert extract_airline_prefix("  EWG123  ") == "EWG"

    def test_none_for_empty_string(self):
        assert extract_airline_prefix("") is None

    def test_none_for_none(self):
        assert extract_airline_prefix(None) is None

    def test_none_for_short_callsign(self):
        assert extract_airline_prefix("AB") is None

    def test_none_for_numeric_prefix(self):
        # Callsigns like "123ABC" have a numeric prefix → not an airline code
        assert extract_airline_prefix("123ABC") is None

    def test_uppercase_normalisation(self):
        assert extract_airline_prefix("dlh001") == "DLH"


# ---------------------------------------------------------------------------
# decompose_arrival_time
# ---------------------------------------------------------------------------


class TestDecomposeArrivalTime:
    def test_returns_all_keys(self):
        ts = 1700004600  # 2023-11-14 22:30:00 UTC
        result = decompose_arrival_time(ts)
        expected_keys = {
            "arrival_date",
            "arrival_time",
            "arrival_year",
            "arrival_month",
            "arrival_day",
            "arrival_hour",
            "arrival_minute",
        }
        assert set(result.keys()) == expected_keys

    def test_date_format(self):
        ts = 1700004600
        result = decompose_arrival_time(ts)
        # date must be YYYY-MM-DD
        parts = result["arrival_date"].split("-")
        assert len(parts) == 3
        assert len(parts[0]) == 4  # year
        assert len(parts[1]) == 2  # month
        assert len(parts[2]) == 2  # day

    def test_time_format(self):
        ts = 1700004600
        result = decompose_arrival_time(ts)
        parts = result["arrival_time"].split(":")
        assert len(parts) == 3

    def test_numeric_components(self):
        ts = 1700004600
        result = decompose_arrival_time(ts)
        assert isinstance(result["arrival_year"], int)
        assert isinstance(result["arrival_month"], int)
        assert isinstance(result["arrival_day"], int)
        assert isinstance(result["arrival_hour"], int)
        assert isinstance(result["arrival_minute"], int)


# ---------------------------------------------------------------------------
# transform_flight
# ---------------------------------------------------------------------------


class TestTransformFlight:
    def test_valid_record_produces_expected_keys(self):
        result = transform_flight(VALID_RAW, "BER")
        assert result is not None
        expected_keys = {
            "icao24",
            "callsign",
            "departure_airport",
            "arrival_airport_icao",
            "arrival_airport_iata",
            "arrival_timestamp",
            "airline_prefix",
            "airline_name",
            "arrival_date",
            "arrival_time",
            "arrival_year",
            "arrival_month",
            "arrival_day",
            "arrival_hour",
            "arrival_minute",
        }
        assert set(result.keys()) == expected_keys

    def test_icao24_is_lowercase(self):
        result = transform_flight(VALID_RAW, "BER")
        assert result["icao24"] == result["icao24"].lower()

    def test_callsign_is_stripped(self):
        result = transform_flight(VALID_RAW, "BER")
        assert result["callsign"] == "DLH001"

    def test_known_airline_name_resolved(self):
        result = transform_flight(VALID_RAW, "BER")
        assert result["airline_prefix"] == "DLH"
        assert result["airline_name"] == AIRLINE_NAMES["DLH"]

    def test_unknown_airline_name_is_none(self):
        raw = dict(VALID_RAW, callsign="XYZ999")
        result = transform_flight(raw, "BER")
        assert result["airline_prefix"] == "XYZ"
        assert result["airline_name"] is None

    def test_returns_none_when_icao24_missing(self):
        raw = dict(VALID_RAW)
        del raw["icao24"]
        assert transform_flight(raw, "BER") is None

    def test_returns_none_when_callsign_missing(self):
        raw = dict(VALID_RAW, callsign=None)
        assert transform_flight(raw, "BER") is None

    def test_returns_none_when_callsign_whitespace(self):
        raw = dict(VALID_RAW, callsign="   ")
        assert transform_flight(raw, "BER") is None

    def test_returns_none_when_lastSeen_missing(self):
        raw = dict(VALID_RAW)
        del raw["lastSeen"]
        assert transform_flight(raw, "BER") is None

    def test_missing_departure_airport_becomes_unknown(self):
        raw = dict(VALID_RAW, estDepartureAirport=None)
        result = transform_flight(raw, "BER")
        assert result["departure_airport"] == "UNKNOWN"

    def test_airport_iata_code_preserved(self):
        result = transform_flight(VALID_RAW, "STR")
        assert result["arrival_airport_iata"] == "STR"


# ---------------------------------------------------------------------------
# transform  (orchestrator)
# ---------------------------------------------------------------------------


class TestTransform:
    def test_filters_invalid_records(self):
        raw_data = {
            "BER": [VALID_RAW, {"icao24": None, "callsign": None}],
            "STR": [],
            "CDG": [dict(VALID_RAW, callsign="AFR100", estArrivalAirport="LFPG")],
        }
        result = transform(raw_data)
        assert len(result) == 2

    def test_empty_input_returns_empty_list(self):
        assert transform({"BER": [], "STR": [], "CDG": []}) == []

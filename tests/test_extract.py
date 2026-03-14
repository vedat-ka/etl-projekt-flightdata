"""Unit tests for etl.extract."""

from unittest.mock import MagicMock, patch

import pytest

from etl.extract import AIRPORTS, extract, fetch_arrivals


# ---------------------------------------------------------------------------
# fetch_arrivals
# ---------------------------------------------------------------------------


class TestFetchArrivals:
    def test_returns_list_on_success(self):
        mock_response = MagicMock()
        mock_response.json.return_value = [{"icao24": "abc123"}]
        mock_response.raise_for_status.return_value = None

        with patch("etl.extract.requests.get", return_value=mock_response) as mock_get:
            result = fetch_arrivals("EDDB", 1700000000, 1700003600)

        assert result == [{"icao24": "abc123"}]
        mock_get.assert_called_once()
        call_kwargs = mock_get.call_args
        assert call_kwargs[1]["params"]["airport"] == "EDDB"
        assert call_kwargs[1]["params"]["begin"] == 1700000000
        assert call_kwargs[1]["params"]["end"] == 1700003600

    def test_returns_empty_list_when_api_returns_none(self):
        mock_response = MagicMock()
        mock_response.json.return_value = None
        mock_response.raise_for_status.return_value = None

        with patch("etl.extract.requests.get", return_value=mock_response):
            result = fetch_arrivals("EDDB", 1700000000, 1700003600)

        assert result == []

    def test_raises_on_http_error(self):
        import requests as req_lib

        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = req_lib.HTTPError("404")

        with patch("etl.extract.requests.get", return_value=mock_response):
            with pytest.raises(req_lib.HTTPError):
                fetch_arrivals("EDDB", 1700000000, 1700003600)


# ---------------------------------------------------------------------------
# extract
# ---------------------------------------------------------------------------


class TestExtract:
    def test_returns_data_for_all_airports(self):
        sample_flight = {"icao24": "abc123", "callsign": "DLH001"}

        def fake_fetch(airport_icao, begin, end):
            return [sample_flight]

        with patch("etl.extract.fetch_arrivals", side_effect=fake_fetch):
            result = extract(window_seconds=3600)

        assert set(result.keys()) == set(AIRPORTS.keys())
        for iata in AIRPORTS:
            assert result[iata] == [sample_flight]

    def test_returns_empty_list_on_error(self):
        def fake_fetch(airport_icao, begin, end):
            raise ConnectionError("network failure")

        with patch("etl.extract.fetch_arrivals", side_effect=fake_fetch):
            result = extract(window_seconds=3600)

        for iata in AIRPORTS:
            assert result[iata] == []

    def test_partial_failure_does_not_abort(self):
        call_count = {"n": 0}

        def fake_fetch(airport_icao, begin, end):
            call_count["n"] += 1
            if airport_icao == "EDDB":
                raise ConnectionError("fail")
            return [{"icao24": "xyz"}]

        with patch("etl.extract.fetch_arrivals", side_effect=fake_fetch):
            result = extract(window_seconds=3600)

        assert result["BER"] == []
        assert result["STR"] == [{"icao24": "xyz"}]
        assert result["CDG"] == [{"icao24": "xyz"}]
        assert call_count["n"] == 3

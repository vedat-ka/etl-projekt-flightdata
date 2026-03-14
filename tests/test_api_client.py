import logging
import os
import unittest
from unittest.mock import MagicMock, patch

from src.api_client import AeroDataBoxClient
from src.config import FlightApiQueryConfig


class AeroDataBoxClientTests(unittest.TestCase):
    def setUp(self) -> None:
        self.logger = logging.getLogger("test_api_client")
        self.env_patcher = patch.dict(
            os.environ,
            {
                "RAPIDAPI_KEY": "test-key",
                "RAPIDAPI_HOST": "aerodatabox.p.rapidapi.com",
                "RAPIDAPI_BASE_URL": "https://aerodatabox.p.rapidapi.com",
            },
            clear=False,
        )
        self.env_patcher.start()

    def tearDown(self) -> None:
        self.env_patcher.stop()

    @patch("src.api_client.requests.get")
    def test_fetch_arrivals_uses_optimized_default_query_params(self, mock_get: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.json.return_value = {"arrivals": []}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        client = AeroDataBoxClient(logger=self.logger)
        payload = client.fetch_arrivals("BER", "2026-03-11T08:00", "2026-03-11T12:00")

        self.assertEqual(payload["_query"]["direction"], "Arrival")
        self.assertEqual(payload["_query"]["withCancelled"], "false")
        self.assertEqual(payload["_query"]["withCodeshared"], "false")
        mock_get.assert_called_once_with(
            "https://aerodatabox.p.rapidapi.com/flights/airports/iata/BER/2026-03-11T08:00/2026-03-11T12:00",
            headers=client.headers,
            params={
                "withLeg": "true",
                "direction": "Arrival",
                "withCancelled": "false",
                "withCodeshared": "false",
                "withCargo": "false",
                "withPrivate": "false",
                "withLocation": "false",
            },
            timeout=30,
        )

    @patch("src.api_client.requests.get")
    def test_fetch_arrivals_supports_icao_and_custom_query_params(self, mock_get: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.json.return_value = {"arrivals": []}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        client = AeroDataBoxClient(logger=self.logger)
        query = FlightApiQueryConfig(
            direction="Both",
            with_cancelled=True,
            with_codeshared=True,
            with_cargo=True,
        )

        client.fetch_arrivals(
            "EDDB",
            "2026-03-11T08:00",
            "2026-03-11T12:00",
            code_type="icao",
            query=query,
        )

        self.assertEqual(mock_get.call_args.kwargs["params"]["direction"], "Both")
        self.assertEqual(mock_get.call_args.kwargs["params"]["withCargo"], "true")
        self.assertIn("/icao/EDDB/", mock_get.call_args.args[0])

    def test_fetch_arrivals_rejects_time_windows_longer_than_twelve_hours(self) -> None:
        client = AeroDataBoxClient(logger=self.logger)

        with self.assertRaises(ValueError):
            client.fetch_arrivals("BER", "2026-03-11T08:00", "2026-03-11T21:00")

    def test_base_url_can_be_overridden_from_environment(self) -> None:
        with patch.dict(os.environ, {"RAPIDAPI_BASE_URL": "https://custom.example.com/"}):
            client = AeroDataBoxClient(logger=self.logger)

        self.assertEqual(client.base_url, "https://custom.example.com")


if __name__ == "__main__":
    unittest.main()
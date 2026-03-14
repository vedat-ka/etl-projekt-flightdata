import logging
import os
from datetime import datetime

import requests
from dotenv import load_dotenv

from src.config import FlightApiQueryConfig


class AeroDataBoxClient:
    """OOP wrapper around the AeroDataBox flight API."""

    DEFAULT_BASE_URL = "https://aerodatabox.p.rapidapi.com"

    def __init__(self, logger: logging.Logger, timeout: int = 30):
        load_dotenv()
        self.logger = logger
        self.timeout = timeout
        self.api_key = os.getenv("RAPIDAPI_KEY")
        self.api_host = os.getenv("RAPIDAPI_HOST")
        self.base_url = os.getenv("RAPIDAPI_BASE_URL", self.DEFAULT_BASE_URL).rstrip("/")

        if not self.api_key or not self.api_host:
            raise ValueError("Missing RAPIDAPI_KEY or RAPIDAPI_HOST in environment.")

    @property
    def headers(self) -> dict[str, str]:
        return {
            "x-rapidapi-key": self.api_key,
            "x-rapidapi-host": self.api_host,
        }

    @staticmethod
    def _validate_code(code_type: str, code: str) -> tuple[str, str]:
        normalized_code_type = code_type.lower()
        normalized_code = code.upper()

        expected_length = {"iata": 3, "icao": 4}.get(normalized_code_type)
        if expected_length is None:
            raise ValueError("code_type must be either 'iata' or 'icao'.")
        if len(normalized_code) != expected_length:
            raise ValueError(
                f"{normalized_code_type} code must contain exactly {expected_length} characters."
            )
        return normalized_code_type, normalized_code

    @staticmethod
    def _validate_time_window(from_datetime: str, to_datetime: str) -> None:
        from_local = datetime.strptime(from_datetime, "%Y-%m-%dT%H:%M")
        to_local = datetime.strptime(to_datetime, "%Y-%m-%dT%H:%M")
        window = to_local - from_local

        if window.total_seconds() <= 0:
            raise ValueError("to_datetime must be later than from_datetime.")
        if window.total_seconds() > 12 * 60 * 60:
            raise ValueError("Time window must not exceed 12 hours.")

    @staticmethod
    def _build_query_params(query: FlightApiQueryConfig) -> dict[str, str]:
        return {
            "withLeg": str(query.with_leg).lower(),
            "direction": query.direction,
            "withCancelled": str(query.with_cancelled).lower(),
            "withCodeshared": str(query.with_codeshared).lower(),
            "withCargo": str(query.with_cargo).lower(),
            "withPrivate": str(query.with_private).lower(),
            "withLocation": str(query.with_location).lower(),
        }

    def fetch_arrivals(
        self,
        airport_code: str,
        from_datetime: str,
        to_datetime: str,
        *,
        code_type: str = "iata",
        query: FlightApiQueryConfig | None = None,
    ) -> dict:
        """Fetch arrival flights for one airport and time window."""
        normalized_code_type, normalized_code = self._validate_code(code_type, airport_code)
        self._validate_time_window(from_datetime, to_datetime)
        query_config = query or FlightApiQueryConfig()

        url = (
            f"{self.base_url}/flights/airports/{normalized_code_type}/"
            f"{normalized_code}/{from_datetime}/{to_datetime}"
        )
        params = self._build_query_params(query_config)

        self.logger.info(
            "Requesting %s flights for %s %s from %s to %s with params %s",
            query_config.direction,
            normalized_code_type,
            normalized_code,
            from_datetime,
            to_datetime,
            params,
        )
        response = requests.get(url, headers=self.headers, params=params, timeout=self.timeout)
        response.raise_for_status()

        payload = response.json()
        if isinstance(payload, dict):
            payload.setdefault("_query", {})
            payload["_query"].update(
                {
                    "airport_code": normalized_code,
                    "code_type": normalized_code_type,
                    "from_datetime": from_datetime,
                    "to_datetime": to_datetime,
                    **params,
                }
            )
        return payload

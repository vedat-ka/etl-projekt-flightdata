"""
Extract step of the ETL pipeline.

Fetches arrival data for BER, STR, and CDG from the OpenSky Network API.
"""

import time
import requests

# IATA code → ICAO airport identifier used by OpenSky Network
AIRPORTS = {
    "BER": "EDDB",
    "STR": "EDDS",
    "CDG": "LFPG",
}

OPENSKY_BASE_URL = "https://opensky-network.org/api/flights/arrival"

# Maximum window allowed by the anonymous OpenSky API is 1 hour (3600 s)
DEFAULT_WINDOW_SECONDS = 3600


def fetch_arrivals(airport_icao: str, begin: int, end: int) -> list:
    """Fetch raw arrival records from the OpenSky Network API.

    Parameters
    ----------
    airport_icao:
        ICAO code of the destination airport (e.g. ``"EDDB"``).
    begin:
        Start of the time window as a Unix timestamp (seconds).
    end:
        End of the time window as a Unix timestamp (seconds).

    Returns
    -------
    list
        List of raw flight dicts returned by the API (may be empty).

    Raises
    ------
    requests.HTTPError
        If the API returns a non-2xx status code.
    """
    params = {"airport": airport_icao, "begin": begin, "end": end}
    response = requests.get(OPENSKY_BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.json() or []


def extract(window_seconds: int = DEFAULT_WINDOW_SECONDS) -> dict:
    """Extract arrival data for all configured airports.

    Parameters
    ----------
    window_seconds:
        Length of the time window (in seconds) to look back from now.
        Defaults to one hour to comply with the anonymous API limit.

    Returns
    -------
    dict
        Mapping of IATA code → list of raw flight dicts, e.g.
        ``{"BER": [...], "STR": [...], "CDG": [...]}``.
    """
    end = int(time.time())
    begin = end - window_seconds

    results = {}
    for iata_code, icao_code in AIRPORTS.items():
        try:
            flights = fetch_arrivals(icao_code, begin, end)
            print(f"[extract] {iata_code}: {len(flights)} raw flights")
        except Exception as exc:
            print(f"[extract] {iata_code}: error – {exc}")
            flights = []
        results[iata_code] = flights

    return results

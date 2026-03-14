"""
Transform step of the ETL pipeline.

Cleans raw OpenSky flight records, extracts required fields, decomposes the
arrival timestamp, and normalises airline information.
"""

from datetime import datetime, timezone
from typing import Dict, List, Optional

# Mapping of 3-letter ICAO airline prefixes to human-readable names.
# Extend as needed.
AIRLINE_NAMES: Dict[str, str] = {
    "DLH": "Lufthansa",
    "EWG": "Eurowings",
    "CLH": "Lufthansa CityLine",
    "BAW": "British Airways",
    "AFR": "Air France",
    "KLM": "KLM Royal Dutch Airlines",
    "RYR": "Ryanair",
    "EZY": "easyJet",
    "TUI": "TUI Airways",
    "UAE": "Emirates",
    "QTR": "Qatar Airways",
    "THY": "Turkish Airlines",
    "SWR": "Swiss International Air Lines",
    "AUA": "Austrian Airlines",
    "VLG": "Vueling",
    "IBE": "Iberia",
    "AZA": "ITA Airways",
    "NAX": "Norwegian Air Shuttle",
    "WZZ": "Wizz Air",
    "BTI": "airBaltic",
    "BEL": "Brussels Airlines",
    "SAS": "Scandinavian Airlines",
    "FIN": "Finnair",
    "TAP": "TAP Air Portugal",
    "LOT": "LOT Polish Airlines",
    "CSA": "Czech Airlines",
    "AEE": "Aegean Airlines",
    "TOM": "TUI fly",
    "CFG": "Condor",
    "GWI": "Germania",
    "FDX": "FedEx",
    "UPS": "UPS Airlines",
}

# Fields that must be present and non-null in a raw record
REQUIRED_FIELDS = ("icao24", "callsign", "estArrivalAirport", "lastSeen")


def extract_airline_prefix(callsign: str) -> Optional[str]:
    """Return the 3-letter ICAO airline prefix from *callsign*, or ``None``."""
    cleaned = callsign.strip() if callsign else ""
    if len(cleaned) >= 3:
        prefix = cleaned[:3].upper()
        # Only return letter-only prefixes (ignore numeric-only callsigns)
        if prefix.isalpha():
            return prefix
    return None


def decompose_arrival_time(timestamp: int) -> Dict[str, object]:
    """Decompose a Unix timestamp into individual date/time components.

    Returns a dict with keys:
    ``arrival_date``, ``arrival_time``, ``arrival_year``, ``arrival_month``,
    ``arrival_day``, ``arrival_hour``, ``arrival_minute``.
    """
    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return {
        "arrival_date": dt.strftime("%Y-%m-%d"),
        "arrival_time": dt.strftime("%H:%M:%S"),
        "arrival_year": dt.year,
        "arrival_month": dt.month,
        "arrival_day": dt.day,
        "arrival_hour": dt.hour,
        "arrival_minute": dt.minute,
    }


def transform_flight(raw: dict, airport_iata: str) -> Optional[dict]:
    """Transform a single raw flight record.

    Returns ``None`` if the record is missing required fields or has an
    invalid callsign.
    """
    # Validate required fields
    for field in REQUIRED_FIELDS:
        if not raw.get(field):
            return None

    callsign = raw["callsign"].strip()
    if not callsign:
        return None

    airline_prefix = extract_airline_prefix(callsign)
    airline_name = AIRLINE_NAMES.get(airline_prefix) if airline_prefix else None

    arrival_info = decompose_arrival_time(raw["lastSeen"])

    return {
        "icao24": raw["icao24"].strip().lower(),
        "callsign": callsign,
        "departure_airport": (raw.get("estDepartureAirport") or "UNKNOWN").strip(),
        "arrival_airport_icao": raw["estArrivalAirport"].strip(),
        "arrival_airport_iata": airport_iata,
        "arrival_timestamp": raw["lastSeen"],
        "airline_prefix": airline_prefix,
        "airline_name": airline_name,
        **arrival_info,
    }


def transform(raw_data: Dict[str, list]) -> List[dict]:
    """Transform all raw airport flight data.

    Parameters
    ----------
    raw_data:
        Output of :func:`etl.extract.extract` –
        mapping of IATA code → list of raw flight dicts.

    Returns
    -------
    list
        List of cleaned, normalised flight dicts ready for loading.
    """
    transformed: List[dict] = []
    for airport_iata, flights in raw_data.items():
        for raw_flight in flights:
            result = transform_flight(raw_flight, airport_iata)
            if result is not None:
                transformed.append(result)
    print(f"[transform] {len(transformed)} valid flights after transformation")
    return transformed

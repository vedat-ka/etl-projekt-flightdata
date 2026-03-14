import json
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from src.api_client import AeroDataBoxClient
from src.config import AirportConfig, PipelineConfig, PipelineRunOptions
from src.database import SQLiteManager


class FlightFileStore:
    """Handle raw and processed ETL file persistence."""

    def __init__(
        self,
        raw_dir: Path,
        processed_dir: Path,
        reports_dir: Path,
        logger: logging.Logger,
    ):
        self.raw_dir = raw_dir
        self.processed_dir = processed_dir
        self.reports_dir = reports_dir
        self.logger = logger

    def raw_payload_path(self, airport_iata: str) -> Path:
        return self.raw_dir / f"{airport_iata.lower()}_arrivals_raw.json"

    def save_raw_payloads(self, payloads: dict[str, dict]) -> list[Path]:
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        paths = []
        for airport_iata, payload in payloads.items():
            path = self.raw_payload_path(airport_iata)
            if payload.get("_query", {}).get("fetch_source") == "cache" and path.exists():
                self.logger.info("Raw cache kept for %s at %s", airport_iata, path)
                paths.append(path)
                continue

            with open(path, "w", encoding="utf-8") as file_handle:
                json.dump(payload, file_handle, indent=2)
            paths.append(path)
            self.logger.info("Raw data saved to %s", path)
        return paths

    def load_cached_payload(self, airport_iata: str) -> dict | None:
        path = self.raw_payload_path(airport_iata)
        if not path.exists():
            return None

        with open(path, encoding="utf-8") as file_handle:
            payload = json.load(file_handle)
        self.logger.info("Using cached raw payload for %s from %s", airport_iata, path)
        return payload

    def save_processed(self, df: pd.DataFrame, filename: str) -> Path:
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        path = self.processed_dir / filename
        df.to_csv(path, index=False)
        self.logger.info("Processed CSV saved to %s (%s rows)", path, len(df))
        return path

    def save_report(self, report_text: str, filename: str) -> Path:
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        path = self.reports_dir / filename
        path.write_text(report_text, encoding="utf-8")
        self.logger.info("Run report saved to %s", path)
        return path


class FlightTransformer:
    """Transform raw arrival records into the normalized ETL output schema."""

    REQUIRED_COLUMNS = [
        "destination_airport_iata",
        "destination_airport_name",
        "origin_country",
        "flight_number",
        "airline_name",
        "arrival_time",
    ]

    def __init__(self, logger: logging.Logger):
        self.logger = logger

    @staticmethod
    def _pick_local_time(block: dict, *field_names: str) -> str | None:
        for field_name in field_names:
            value = block.get(field_name, {})
            if isinstance(value, dict) and value.get("local"):
                return value["local"]
        return None

    def extract_records(self, payloads: dict[str, dict]) -> list[dict]:
        records = []
        for airport_iata, payload in payloads.items():
            arrivals = payload.get("arrivals", [])
            self.logger.info("Extracting %s arrival records for %s", len(arrivals), airport_iata)
            for flight in arrivals:
                flight["_query"] = payload.get("_query", {}).copy()
                flight["_query"]["iata"] = airport_iata
                records.append(flight)
        self.logger.info("Extracted %s total raw records", len(records))
        return records

    def transform(self, records: list[dict]) -> pd.DataFrame:
        rows = []
        for record in records:
            dep = record.get("departure", {})
            arr = record.get("arrival", {})
            airline = record.get("airline", {})
            query = record.get("_query", {})

            destination_airport_iata = query.get("iata") or arr.get("airport", {}).get("iata")
            destination_airport_name = query.get("airport_name") or arr.get("airport", {}).get("name")
            origin_airport = dep.get("airport", {})
            arrival_time = self._pick_local_time(arr, "scheduledTime")
            source_record_key = "|".join(
                [
                    destination_airport_iata or "",
                    record.get("number") or "",
                    airline.get("iata") or airline.get("name") or "",
                    origin_airport.get("iata") or "",
                    arrival_time or "",
                ]
            )

            rows.append(
                {
                    "destination_airport_iata": destination_airport_iata,
                    "destination_airport_name": destination_airport_name,
                    "origin_airport_iata": origin_airport.get("iata"),
                    "origin_country": origin_airport.get("countryCode"),
                    "flight_number": record.get("number"),
                    "airline_iata": airline.get("iata") or "",
                    "airline_icao": airline.get("icao") or "",
                    "airline_name": airline.get("name"),
                    "arrival_time": arrival_time,
                    "status": record.get("status"),
                    "source_record_key": source_record_key,
                }
            )

        df = pd.DataFrame(rows)
        if df.empty:
            self.logger.warning("Transformation produced an empty DataFrame.")
            return df

        before_validation = len(df)
        df["arrival_time"] = pd.to_datetime(df["arrival_time"], errors="coerce", utc=True)
        df = df.dropna(subset=self.REQUIRED_COLUMNS).copy()
        self.logger.info(
            "Validation removed %s invalid rows",
            before_validation - len(df),
        )

        before_dedup = len(df)
        df = df.drop_duplicates(subset=["source_record_key"]).copy()
        self.logger.info("Deduplication removed %s rows", before_dedup - len(df))

        df["arrival_year"] = df["arrival_time"].dt.year.astype(int)
        df["arrival_month"] = df["arrival_time"].dt.month.astype(int)
        df["arrival_day"] = df["arrival_time"].dt.day.astype(int)
        df["arrival_hour"] = df["arrival_time"].dt.hour.astype(int)
        df["arrival_minute"] = df["arrival_time"].dt.minute.astype(int)
        df["arrival_second"] = df["arrival_time"].dt.second.astype(int)

        transformed = df.sort_values(
            ["destination_airport_iata", "arrival_time", "flight_number"]
        ).reset_index(drop=True)
        self.logger.info("Transformation produced %s clean rows", len(transformed))
        return transformed


class FlightETLPipeline:
    """Coordinate extract, transform, and load steps for the ETL project."""

    def __init__(
        self,
        config: PipelineConfig,
        client: AeroDataBoxClient,
        file_store: FlightFileStore,
        transformer: FlightTransformer,
        database_manager: SQLiteManager,
        logger: logging.Logger,
        run_options: PipelineRunOptions | None = None,
    ):
        self.config = config
        self.client = client
        self.file_store = file_store
        self.transformer = transformer
        self.database_manager = database_manager
        self.logger = logger
        self.run_options = run_options or PipelineRunOptions()
        self.fetch_results: dict[str, dict[str, str]] = {}

    def _build_fetch_window(self, airport: AirportConfig) -> tuple[str, str]:
        if self.run_options.has_explicit_window:
            return self.run_options.from_datetime, self.run_options.to_datetime

        lookback_hours = self.run_options.lookback_hours
        if lookback_hours is None:
            lookback_hours = self.config.lookback_hours

        lookahead_hours = self.run_options.lookahead_hours
        if lookahead_hours is None:
            lookahead_hours = self.config.lookahead_hours

        now = datetime.now(ZoneInfo(airport.timezone)).replace(second=0, microsecond=0)
        start = now - timedelta(hours=lookback_hours)
        end = now + timedelta(hours=lookahead_hours)
        return start.strftime("%Y-%m-%dT%H:%M"), end.strftime("%Y-%m-%dT%H:%M")

    def _fetch_airport_payload(self, airport: AirportConfig) -> dict | None:
        from_datetime, to_datetime = self._build_fetch_window(airport)
        self.logger.info(
            "[1/3] Preparing arrivals for %s (%s) from %s to %s",
            airport.iata,
            airport.name,
            from_datetime,
            to_datetime,
        )

        cached_payload = None
        if not self.run_options.should_force_refresh(airport.iata):
            cached_payload = self.file_store.load_cached_payload(airport.iata)
        if cached_payload is not None:
            cached_payload.setdefault("_query", {})
            cached_payload["_query"].update(
                {
                    "iata": airport.iata,
                    "airport_name": airport.name,
                    "fetch_source": "cache",
                    "requested_from_datetime": from_datetime,
                    "requested_to_datetime": to_datetime,
                }
            )
            self.fetch_results[airport.iata] = {
                "airport_name": airport.name,
                "fetch_source": "cache",
                "status": "cache_hit",
                "requested_from_datetime": from_datetime,
                "requested_to_datetime": to_datetime,
                "error": "",
            }
            return cached_payload

        payload = None
        last_error = ""
        for attempt in range(1, self.config.retry_attempts + 1):
            try:
                payload = self.client.fetch_arrivals(
                    airport.lookup_code,
                    from_datetime,
                    to_datetime,
                    code_type=airport.api_code_type,
                    query=self.config.api_query,
                )
                break
            except Exception as exc:
                last_error = str(exc)
                if attempt < self.config.retry_attempts:
                    self.logger.warning(
                        "Fetch for %s failed on attempt %s/%s: %s. Retrying in %ss",
                        airport.iata,
                        attempt,
                        self.config.retry_attempts,
                        exc,
                        self.config.retry_delay_seconds,
                    )
                    time.sleep(self.config.retry_delay_seconds)
                else:
                    self.logger.error("Fetch for %s failed: %s", airport.iata, exc)

        if payload is None:
            self.fetch_results[airport.iata] = {
                "airport_name": airport.name,
                "fetch_source": "api",
                "status": "failed",
                "requested_from_datetime": from_datetime,
                "requested_to_datetime": to_datetime,
                "error": last_error,
            }
            return None

        payload.setdefault("_query", {})
        payload["_query"].update(
            {
                "iata": airport.iata,
                "airport_name": airport.name,
                "airport_lookup_code": airport.lookup_code,
                "airport_lookup_code_type": airport.api_code_type,
                "from_datetime": from_datetime,
                "to_datetime": to_datetime,
                "fetch_source": "api",
            }
        )
        self.fetch_results[airport.iata] = {
            "airport_name": airport.name,
            "fetch_source": "api",
            "status": "fetched",
            "requested_from_datetime": from_datetime,
            "requested_to_datetime": to_datetime,
            "error": "",
        }
        return payload

    def fetch_all_airports(self) -> dict[str, dict]:
        payloads = {}
        selected_airports = [
            airport for airport in self.config.airports if self.run_options.includes_airport(airport.iata)
        ]

        for airport in selected_airports:
            payload = self._fetch_airport_payload(airport)
            if payload is not None:
                payloads[airport.iata] = payload

        if not payloads:
            raise RuntimeError("No airport data could be fetched.")
        if len(payloads) < len(selected_airports):
            self.logger.warning(
                "Only %s of %s airports were available for this run.",
                len(payloads),
                len(selected_airports),
            )
        return payloads

    def log_summary(self, df: pd.DataFrame, load_stats: dict) -> None:
        flights_per_airport = df["destination_airport_iata"].value_counts().sort_index()
        top_origin_countries = df["origin_country"].value_counts().head(10)
        top_airlines = df["airline_name"].value_counts().head(10)
        status_distribution = df["status"].fillna("UNKNOWN").value_counts()
        fetch_overview = pd.DataFrame.from_dict(self.fetch_results, orient="index")

        self.logger.info("Processed rows: %s", len(df))
        self.logger.info("Unique airlines: %s", df["airline_name"].nunique())
        self.logger.info("SQLite database: %s", load_stats["db_path"])
        self.logger.info("Airlines in master table: %s", load_stats["airline_count"])
        self.logger.info("Flights in flights table: %s", load_stats["flight_count"])
        if not fetch_overview.empty:
            self.logger.info("Fetch overview:\n%s", fetch_overview.to_string())
        self.logger.info("Flights per destination airport:\n%s", flights_per_airport.to_string())
        self.logger.info("Status distribution:\n%s", status_distribution.to_string())
        self.logger.info("Top origin countries:\n%s", top_origin_countries.to_string())
        self.logger.info("Top airlines:\n%s", top_airlines.to_string())

    def build_report(self, df: pd.DataFrame, load_stats: dict) -> str:
        flights_per_airport = df["destination_airport_iata"].value_counts().sort_index()
        status_distribution = df["status"].fillna("UNKNOWN").value_counts()
        top_origin_countries = df["origin_country"].value_counts().head(10)
        top_airlines = df["airline_name"].value_counts().head(10)
        fetch_overview = pd.DataFrame.from_dict(self.fetch_results, orient="index")

        if fetch_overview.empty:
            fetch_overview_text = "No fetch metadata available."
        else:
            fetch_overview = fetch_overview[
                [
                    "airport_name",
                    "fetch_source",
                    "status",
                    "requested_from_datetime",
                    "requested_to_datetime",
                    "error",
                ]
            ]
            fetch_overview.index.name = "airport_iata"
            fetch_overview_text = fetch_overview.to_string()

        lines = [
            "# ETL Run Report",
            "",
            f"- Processed rows: {len(df)}",
            f"- Unique airlines in current snapshot: {df['airline_name'].nunique()}",
            f"- SQLite database: {load_stats['db_path']}",
            f"- Airlines in master table: {load_stats['airline_count']}",
            f"- Flights in flights table: {load_stats['flight_count']}",
            "",
            "## Fetch overview",
            "",
            "```text",
            fetch_overview_text,
            "```",
            "",
            "## Flights per destination airport",
            "",
            "```text",
            flights_per_airport.to_string(),
            "```",
            "",
            "## Status distribution",
            "",
            "```text",
            status_distribution.to_string(),
            "```",
            "",
            "## Top origin countries",
            "",
            "```text",
            top_origin_countries.to_string(),
            "```",
            "",
            "## Top airlines",
            "",
            "```text",
            top_airlines.to_string(),
            "```",
        ]
        return "\n".join(lines) + "\n"

    def run(self) -> tuple[pd.DataFrame, dict]:
        self.logger.info("=== Flight ETL Pipeline ===")
        payloads = self.fetch_all_airports()

        self.logger.info("[2/3] Running ETL pipeline")
        self.file_store.save_raw_payloads(payloads)
        records = self.transformer.extract_records(payloads)
        df = self.transformer.transform(records)
        self.file_store.save_processed(df, self.config.processed_filename)

        self.logger.info("[3/3] Loading transformed rows into SQLite")
        load_stats = self.database_manager.load_flights(df)
        self.log_summary(df, load_stats)
        report_text = self.build_report(df, load_stats)
        self.file_store.save_report(report_text, self.config.report_filename)
        self.logger.info("ETL pipeline finished successfully")
        return df, load_stats

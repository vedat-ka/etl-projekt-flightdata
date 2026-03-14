import json
import os
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from backend.schemas import DashboardSettingsPayload, ETLRunRequest, SQLExplorerRequest, SQLQueryRequest
from src.api_client import AeroDataBoxClient
from src.config import AirportConfig, PipelineConfig, PipelineRunOptions
from src.database import SQLiteManager
from src.etl import FlightETLPipeline, FlightFileStore, FlightTransformer
from src.logger_config import setup_logging


class DashboardService:
    IGNORED_PATH_PARTS = {".venv", "node_modules", ".git", "__pycache__", "dist"}

    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.config = PipelineConfig.default(base_dir)
        self.logger = setup_logging(self.config.log_path)
        self.settings_path = self.config.data_dir / "dashboard_settings.json"
        self.database_manager = SQLiteManager(db_path=self.config.db_path, logger=self.logger)

    def _available_airport_codes(self) -> set[str]:
        return {airport.iata for airport in self.config.airports}

    @staticmethod
    def _is_valid_custom_airport_code(code: str) -> bool:
        normalized = code.strip().upper()
        return len(normalized) == 3 and normalized.isalpha()

    def _custom_airport_config(self, airport_code: str) -> dict:
        return {
            "iata": airport_code,
            "name": f"Custom {airport_code}",
            "timezone": "UTC",
            "api_code": airport_code,
            "api_code_type": "iata",
        }

    def _airport_catalog(self) -> dict[str, dict]:
        catalog = {}
        for airport in self.config.airports:
            catalog[airport.iata] = {
                "iata": airport.iata,
                "name": airport.name,
                "timezone": airport.timezone,
                "api_code": airport.lookup_code,
                "api_code_type": airport.api_code_type,
            }

        stored_settings = self._read_json_file(self.settings_path) or {}
        for airport_code in stored_settings.get("selected_airports", []):
            normalized_code = airport_code.strip().upper()
            if normalized_code not in catalog and self._is_valid_custom_airport_code(normalized_code):
                catalog[normalized_code] = self._custom_airport_config(normalized_code)

        return catalog

    def _normalize_airport_codes(self, airport_codes: list[str]) -> list[str]:
        normalized = []
        for code in airport_codes:
            upper_code = code.strip().upper()
            if not upper_code:
                continue
            if (upper_code in self._available_airport_codes() or self._is_valid_custom_airport_code(upper_code)) and upper_code not in normalized:
                normalized.append(upper_code)
        return normalized

    def _default_settings(self) -> dict:
        return {
            "selected_airports": [airport.iata for airport in self.config.airports],
            "lookback_hours": self.config.lookback_hours,
            "lookahead_hours": self.config.lookahead_hours,
        }

    def _read_json_file(self, path: Path) -> dict | None:
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            self.logger.warning("Could not read JSON file %s", path)
            return None

    def _write_json_file(self, path: Path, payload: dict) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def get_airports(self) -> list[dict]:
        return list(self._airport_catalog().values())

    def get_settings(self) -> dict:
        settings = self._default_settings()
        stored_settings = self._read_json_file(self.settings_path)
        if stored_settings is None:
            return settings

        selected_airports = self._normalize_airport_codes(
            stored_settings.get("selected_airports", settings["selected_airports"])
        )
        if not selected_airports:
            selected_airports = settings["selected_airports"]

        return {
            "selected_airports": selected_airports,
            "lookback_hours": int(stored_settings.get("lookback_hours", settings["lookback_hours"])),
            "lookahead_hours": int(stored_settings.get("lookahead_hours", settings["lookahead_hours"])),
        }

    def update_settings(self, payload: DashboardSettingsPayload) -> dict:
        settings = {
            "selected_airports": self._normalize_airport_codes(payload.selected_airports),
            "lookback_hours": payload.lookback_hours,
            "lookahead_hours": payload.lookahead_hours,
        }
        if not settings["selected_airports"]:
            settings["selected_airports"] = self._default_settings()["selected_airports"]
        self._write_json_file(self.settings_path, settings)
        return settings

    def get_health(self, job_state: dict) -> dict:
        load_dotenv()
        api_key = os.getenv("RAPIDAPI_KEY")
        api_host = os.getenv("RAPIDAPI_HOST")

        return {
            "backend_status": "running",
            "python_version": sys.version.split()[0],
            "api_credentials_configured": bool(api_key and api_host),
            "database_exists": self.config.db_path.exists(),
            "log_exists": self.config.log_path.exists(),
            "report_exists": self.config.report_path.exists(),
            "job_status": job_state["status"],
        }

    def _relative_path(self, path: Path) -> str:
        return str(path.relative_to(self.base_dir)).replace("\\", "/")

    def _should_ignore_path(self, path: Path) -> bool:
        return any(path_part in self.IGNORED_PATH_PARTS for path_part in path.parts)

    @staticmethod
    def _top_items(series: pd.Series, *, limit: int = 8) -> list[dict]:
        counts = series.fillna("UNKNOWN").astype(str).replace("", "UNKNOWN").value_counts().head(limit)
        return [
            {
                "name": str(name),
                "count": int(count),
            }
            for name, count in counts.items()
        ]

    def _build_csv_analytics(self, df: pd.DataFrame) -> dict:
        chart_groups = []
        preferred_columns = [
            ("destination_airport_iata", "Flights pro Zielflughafen"),
            ("airline_name", "Flights pro Airline"),
            ("origin_country", "Flights pro Herkunftsland"),
            ("status", "Statusverteilung"),
        ]

        for column_name, label in preferred_columns:
            if column_name not in df.columns:
                continue
            items = self._top_items(df[column_name])
            if items:
                chart_groups.append(
                    {
                        "column": column_name,
                        "label": label,
                        "items": items,
                    }
                )

        if not chart_groups:
            fallback_columns = [
                column_name
                for column_name in df.columns
                if df[column_name].dtype == "object" or str(df[column_name].dtype).startswith("string")
            ][:2]
            for column_name in fallback_columns:
                items = self._top_items(df[column_name])
                if items:
                    chart_groups.append(
                        {
                            "column": column_name,
                            "label": f"Top-Werte fuer {column_name}",
                            "items": items,
                        }
                    )

        return {
            "row_count": int(len(df.index)),
            "chart_groups": chart_groups,
        }

    @staticmethod
    def _build_csv_preview_rows(df: pd.DataFrame, analytics: dict, limit: int = 36) -> list[dict]:
        if df.empty:
            return []

        selected_indices: list[int] = []
        seen_indices: set[int] = set()

        for chart_group in analytics.get("chart_groups", []):
            column_name = chart_group.get("column")
            if column_name not in df.columns:
                continue

            for item in chart_group.get("items", [])[:4]:
                matching_indices = df.index[df[column_name].astype(str) == str(item.get("name", ""))].tolist()
                for index in matching_indices[:4]:
                    if index not in seen_indices:
                        seen_indices.add(index)
                        selected_indices.append(index)
                    if len(selected_indices) >= limit:
                        break
                if len(selected_indices) >= limit:
                    break
            if len(selected_indices) >= limit:
                break

        if len(selected_indices) < limit:
            for index in df.index.tolist():
                if index in seen_indices:
                    continue
                seen_indices.add(index)
                selected_indices.append(index)
                if len(selected_indices) >= limit:
                    break

        preview_df = df.loc[selected_indices].copy()
        return preview_df.to_dict(orient="records")

    def _build_log_analytics(self) -> dict:
        if not self.config.log_path.exists():
            return {
                "line_count": 0,
                "level_counts": [],
                "hourly_activity": [],
                "recent_events": [],
            }

        level_counter: Counter[str] = Counter()
        hourly_counter: Counter[str] = Counter()
        recent_events = []
        lines = self.config.log_path.read_text(encoding="utf-8").splitlines()

        for line in lines:
            parts = [part.strip() for part in line.split("|", maxsplit=3)]
            if len(parts) != 4:
                continue

            timestamp_text, level, logger_name, message = parts
            level_counter[level] += 1
            try:
                timestamp = datetime.strptime(timestamp_text, "%Y-%m-%d %H:%M:%S")
                hourly_counter[timestamp.strftime("%d.%m %H:00")] += 1
            except ValueError:
                timestamp = None

            recent_events.append(
                {
                    "timestamp": timestamp_text,
                    "level": level,
                    "logger": logger_name,
                    "message": message,
                }
            )

        hourly_activity = [
            {
                "bucket": bucket,
                "count": int(count),
            }
            for bucket, count in sorted(hourly_counter.items())[-12:]
        ]

        return {
            "line_count": int(len(lines)),
            "level_counts": [
                {
                    "name": level,
                    "count": int(count),
                }
                for level, count in level_counter.most_common()
            ],
            "hourly_activity": hourly_activity,
            "recent_events": recent_events[-12:],
        }

    def _collect_csv_files(self) -> list[dict]:
        csv_files = []
        for path in sorted(self.base_dir.rglob("*.csv")):
            if self._should_ignore_path(path) or not path.is_file():
                continue
            full_df = pd.read_csv(path).fillna("")
            analytics = self._build_csv_analytics(full_df)
            csv_files.append(
                {
                    "name": path.name,
                    "relative_path": self._relative_path(path),
                    "size_bytes": path.stat().st_size,
                    "updated_at": path.stat().st_mtime,
                    "row_count": int(len(full_df.index)),
                    "columns": full_df.columns.tolist(),
                    "preview_rows": self._build_csv_preview_rows(full_df, analytics),
                    "analytics": analytics,
                }
            )
        return csv_files

    def _collect_raw_payloads(self) -> list[dict]:
        raw_payloads = []
        for path in sorted(self.config.raw_dir.glob("*.json")):
            payload = self._read_json_file(path) or {}
            raw_payloads.append(
                {
                    "name": path.name,
                    "relative_path": self._relative_path(path),
                    "size_bytes": path.stat().st_size,
                    "updated_at": path.stat().st_mtime,
                    "airport_iata": payload.get("_query", {}).get("iata", path.stem[:3].upper()),
                    "arrival_count": len(payload.get("arrivals", [])),
                    "fetch_source": payload.get("_query", {}).get("fetch_source", "unknown"),
                }
            )
        return raw_payloads

    def _collect_reports(self) -> list[dict]:
        reports = []
        for path in sorted(self.config.reports_dir.glob("*.md")):
            reports.append(
                {
                    "name": path.name,
                    "relative_path": self._relative_path(path),
                    "size_bytes": path.stat().st_size,
                    "updated_at": path.stat().st_mtime,
                }
            )
        return reports

    def _collect_logs(self) -> list[dict]:
        logs = []
        for path in sorted(self.config.log_dir.glob("*.log")):
            logs.append(
                {
                    "name": path.name,
                    "relative_path": self._relative_path(path),
                    "size_bytes": path.stat().st_size,
                    "updated_at": path.stat().st_mtime,
                }
            )
        return logs

    def get_file_overview(self) -> dict:
        return {
            "csv_files": self._collect_csv_files(),
            "raw_payloads": self._collect_raw_payloads(),
            "reports": self._collect_reports(),
            "logs": self._collect_logs(),
        }

    def get_log_tail(self, lines: int = 120) -> str:
        if not self.config.log_path.exists():
            return ""
        content = self.config.log_path.read_text(encoding="utf-8").splitlines()
        return "\n".join(content[-max(1, lines):])

    def get_latest_report(self) -> str:
        if not self.config.report_path.exists():
            return ""
        return self.config.report_path.read_text(encoding="utf-8")

    def get_database_snapshot(self) -> dict:
        return self.database_manager.get_dashboard_snapshot()

    def get_database_explorer(self, payload: SQLExplorerRequest) -> dict:
        return self.database_manager.get_explorer_snapshot(payload.table_name, limit=payload.limit)

    def run_sql_query(self, payload: SQLQueryRequest) -> dict:
        return self.database_manager.run_read_only_query(payload.sql)

    def get_dashboard_snapshot(self, job_state: dict) -> dict:
        return {
            "health": self.get_health(job_state),
            "job": job_state,
            "airports": self.get_airports(),
            "settings": self.get_settings(),
            "database": self.get_database_snapshot(),
            "files": self.get_file_overview(),
            "log_analytics": self._build_log_analytics(),
            "log_tail": self.get_log_tail(),
            "report_markdown": self.get_latest_report(),
        }

    def run_etl(self, payload: ETLRunRequest) -> dict:
        selected_airports = self._normalize_airport_codes(payload.selected_airports)
        force_refresh_airports = [
            code for code in self._normalize_airport_codes(payload.force_refresh_airports) if code in selected_airports
        ]
        airport_catalog = self._airport_catalog()

        runtime_airports = []
        configured_airport_codes = self._available_airport_codes()
        for airport_code in selected_airports:
            if airport_code in configured_airport_codes:
                runtime_airports.append(next(airport for airport in self.config.airports if airport.iata == airport_code))
                continue

            airport_meta = airport_catalog.get(airport_code, self._custom_airport_config(airport_code))
            runtime_airports.append(
                AirportConfig(
                    iata=airport_code,
                    name=airport_meta["name"],
                    timezone=airport_meta["timezone"],
                    api_code=airport_meta["api_code"],
                    api_code_type=airport_meta["api_code_type"],
                )
            )

        runtime_config = PipelineConfig(
            base_dir=self.config.base_dir,
            airports=tuple(runtime_airports),
            api_query=self.config.api_query,
            lookback_hours=self.config.lookback_hours,
            lookahead_hours=self.config.lookahead_hours,
            retry_attempts=self.config.retry_attempts,
            retry_delay_seconds=self.config.retry_delay_seconds,
            processed_filename=self.config.processed_filename,
            log_filename=self.config.log_filename,
            db_filename=self.config.db_filename,
            report_filename=self.config.report_filename,
        )

        client = AeroDataBoxClient(logger=self.logger)
        file_store = FlightFileStore(
            raw_dir=runtime_config.raw_dir,
            processed_dir=runtime_config.processed_dir,
            reports_dir=runtime_config.reports_dir,
            logger=self.logger,
        )
        transformer = FlightTransformer(logger=self.logger)
        pipeline = FlightETLPipeline(
            config=runtime_config,
            client=client,
            file_store=file_store,
            transformer=transformer,
            database_manager=self.database_manager,
            logger=self.logger,
            run_options=PipelineRunOptions(
                selected_airports=tuple(selected_airports),
                lookback_hours=payload.lookback_hours,
                lookahead_hours=payload.lookahead_hours,
                from_datetime=payload.from_datetime,
                to_datetime=payload.to_datetime,
                force_refresh_airports=tuple(force_refresh_airports),
            ),
        )

        df, load_stats = pipeline.run()
        return {
            "rows_processed": int(len(df)),
            "load_stats": load_stats,
            "fetch_results": pipeline.fetch_results,
            "processed_csv": self._relative_path(runtime_config.processed_dir / runtime_config.processed_filename),
            "report_path": self._relative_path(runtime_config.report_path),
        }
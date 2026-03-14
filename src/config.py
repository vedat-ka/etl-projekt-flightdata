from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class AirportConfig:
    iata: str
    name: str
    timezone: str
    api_code: str | None = None
    api_code_type: str = "iata"

    @property
    def lookup_code(self) -> str:
        return (self.api_code or self.iata).upper()


@dataclass(frozen=True)
class FlightApiQueryConfig:
    with_leg: bool = True
    direction: str = "Arrival"
    with_cancelled: bool = False
    with_codeshared: bool = False
    with_cargo: bool = False
    with_private: bool = False
    with_location: bool = False

    def __post_init__(self) -> None:
        valid_directions = {"Arrival", "Departure", "Both"}
        if self.direction not in valid_directions:
            raise ValueError(
                f"direction must be one of {sorted(valid_directions)}, got {self.direction!r}."
            )


@dataclass(frozen=True)
class PipelineRunOptions:
    selected_airports: tuple[str, ...] | None = None
    lookback_hours: int | None = None
    lookahead_hours: int | None = None
    from_datetime: str | None = None
    to_datetime: str | None = None
    force_refresh_airports: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if (self.from_datetime is None) != (self.to_datetime is None):
            raise ValueError("from_datetime and to_datetime must be provided together.")
        if self.lookback_hours is not None and self.lookback_hours < 0:
            raise ValueError("lookback_hours must be zero or greater.")
        if self.lookahead_hours is not None and self.lookahead_hours < 0:
            raise ValueError("lookahead_hours must be zero or greater.")

        normalized_selected = None
        if self.selected_airports is not None:
            normalized_selected = tuple(code.upper() for code in self.selected_airports)

        normalized_force_refresh = tuple(code.upper() for code in self.force_refresh_airports)

        object.__setattr__(self, "selected_airports", normalized_selected)
        object.__setattr__(self, "force_refresh_airports", normalized_force_refresh)

    def includes_airport(self, airport_iata: str) -> bool:
        if self.selected_airports is None:
            return True
        return airport_iata.upper() in self.selected_airports

    def should_force_refresh(self, airport_iata: str) -> bool:
        return airport_iata.upper() in self.force_refresh_airports

    @property
    def has_explicit_window(self) -> bool:
        return self.from_datetime is not None and self.to_datetime is not None


@dataclass(frozen=True)
class PipelineConfig:
    base_dir: Path
    airports: tuple[AirportConfig, ...]
    api_query: FlightApiQueryConfig = field(default_factory=FlightApiQueryConfig)
    lookback_hours: int = 3
    lookahead_hours: int = 9
    retry_attempts: int = 2
    retry_delay_seconds: int = 2
    processed_filename: str = "flights_processed.csv"
    log_filename: str = "etl.log"
    db_filename: str = "flights.sqlite"
    report_filename: str = "latest_run_report.md"

    @property
    def data_dir(self) -> Path:
        return self.base_dir / "data"

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def processed_dir(self) -> Path:
        return self.data_dir / "processed"

    @property
    def db_path(self) -> Path:
        return self.data_dir / self.db_filename

    @property
    def log_dir(self) -> Path:
        return self.base_dir / "logs"

    @property
    def log_path(self) -> Path:
        return self.log_dir / self.log_filename

    @property
    def reports_dir(self) -> Path:
        return self.base_dir / "reports"

    @property
    def report_path(self) -> Path:
        return self.reports_dir / self.report_filename

    @classmethod
    def default(cls, base_dir: Path) -> "PipelineConfig":
        return cls(
            base_dir=base_dir,
            airports=(
                AirportConfig("BER", "Berlin Brandenburg", "Europe/Berlin"),
                AirportConfig("STR", "Stuttgart", "Europe/Berlin"),
                AirportConfig("CDG", "Paris Charles de Gaulle", "Europe/Paris"),
            ),
        )
from pydantic import BaseModel, Field, model_validator


class DashboardSettingsPayload(BaseModel):
    selected_airports: list[str] = Field(min_length=1)
    lookback_hours: int = Field(ge=0, le=12)
    lookahead_hours: int = Field(ge=0, le=12)


class ETLRunRequest(BaseModel):
    selected_airports: list[str] = Field(min_length=1)
    lookback_hours: int = Field(default=3, ge=0, le=12)
    lookahead_hours: int = Field(default=9, ge=0, le=12)
    from_datetime: str | None = None
    to_datetime: str | None = None
    force_refresh_airports: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_time_window(self) -> "ETLRunRequest":
        if bool(self.from_datetime) != bool(self.to_datetime):
            raise ValueError("from_datetime and to_datetime must be set together.")
        return self


class SQLQueryRequest(BaseModel):
    sql: str = Field(min_length=1, max_length=4000)


class SQLExplorerRequest(BaseModel):
    table_name: str = Field(min_length=1, max_length=120)
    limit: int = Field(default=50, ge=1, le=200)
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock, Thread

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from backend.schemas import DashboardSettingsPayload, ETLRunRequest, SQLExplorerRequest, SQLQueryRequest
from backend.services import DashboardService


class ETLJobManager:
    def __init__(self):
        self._lock = Lock()
        self._thread: Thread | None = None
        self._state = {
            "status": "idle",
            "started_at": None,
            "finished_at": None,
            "request": None,
            "result": None,
            "error": None,
        }

    def snapshot(self) -> dict:
        with self._lock:
            return deepcopy(self._state)

    def start(self, service: DashboardService, payload: ETLRunRequest) -> dict:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                raise RuntimeError("An ETL job is already running.")

            self._state = {
                "status": "running",
                "started_at": datetime.now(timezone.utc).isoformat(),
                "finished_at": None,
                "request": payload.model_dump(),
                "result": None,
                "error": None,
            }
            self._thread = Thread(target=self._run, args=(service, payload), daemon=True)
            self._thread.start()
            return deepcopy(self._state)

    def _run(self, service: DashboardService, payload: ETLRunRequest) -> None:
        try:
            result = service.run_etl(payload)
            with self._lock:
                self._state["status"] = "success"
                self._state["result"] = result
                self._state["finished_at"] = datetime.now(timezone.utc).isoformat()
        except Exception as exc:
            with self._lock:
                self._state["status"] = "failed"
                self._state["error"] = str(exc)
                self._state["finished_at"] = datetime.now(timezone.utc).isoformat()


base_dir = Path(__file__).resolve().parents[1]
service = DashboardService(base_dir)
job_manager = ETLJobManager()

app = FastAPI(title="Flight ETL Dashboard API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/health")
def get_health() -> dict:
    return service.get_health(job_manager.snapshot())


@app.get("/api/dashboard")
def get_dashboard() -> dict:
    return service.get_dashboard_snapshot(job_manager.snapshot())


@app.get("/api/settings")
def get_settings() -> dict:
    return service.get_settings()


@app.put("/api/settings")
def update_settings(payload: DashboardSettingsPayload) -> dict:
    return service.update_settings(payload)


@app.get("/api/database")
def get_database() -> dict:
    return service.get_database_snapshot()


@app.post("/api/database/explorer")
def get_database_explorer(payload: SQLExplorerRequest) -> dict:
    try:
        return service.get_database_explorer(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/sql/query")
def run_sql_query(payload: SQLQueryRequest) -> dict:
    try:
        return service.run_sql_query(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/files")
def get_files() -> dict:
    return service.get_file_overview()


@app.get("/api/logs")
def get_logs(lines: int = Query(default=120, ge=10, le=500)) -> dict:
    return {"content": service.get_log_tail(lines=lines)}


@app.get("/api/report")
def get_report() -> dict:
    return {"content": service.get_latest_report()}


@app.get("/api/etl/status")
def get_etl_status() -> dict:
    return job_manager.snapshot()


@app.post("/api/etl/run", status_code=202)
def run_etl(payload: ETLRunRequest) -> dict:
    try:
        return job_manager.start(service, payload)
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


frontend_dist = base_dir / "frontend" / "dist"
if frontend_dist.exists():
    app.mount("/", StaticFiles(directory=frontend_dist, html=True), name="dashboard")
"""PDF document ingestion API — manifest inspection, run trigger, live status, logs."""
from __future__ import annotations

import logging
import threading
import uuid
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from src.lookthrough.auth.dependencies import get_current_user
from src.lookthrough.db.models import User
from src.lookthrough.db.repository import ensure_tables
from src.lookthrough.ingestion.ingest_pdf_documents import (
    DEFAULT_BASE_FOLDER,
    FUND_CONFIG,
    MANIFEST_PATH,
    _file_hash,
    _load_manifest,
    _run_migrations,
    _save_manifest,
    ingest_fund_folder,
)

router = APIRouter(prefix="/api/ingestion", tags=["ingestion"])

# ---------------------------------------------------------------------------
# In-memory run state  (ephemeral — resets on server restart)
# ---------------------------------------------------------------------------

_IDLE_PROGRESS: dict = {
    "total_funds": 0,
    "funds_complete": 0,
    "current_fund": None,
    "total_files": 0,
    "files_processed": 0,
    "files_skipped": 0,
    "files_new": 0,
    "holdings_extracted": 0,
}

_run_state: dict = {
    "run_id": None,
    "status": "idle",
    "started_at": None,
    "completed_at": None,
    "progress": dict(_IDLE_PROGRESS),
    "error": None,
    "last_result": None,
}

_state_lock = threading.Lock()
_log_lines: list[dict] = []
_MAX_LOG_LINES = 200


# ---------------------------------------------------------------------------
# Custom log handler — captures ingestion module logs in memory
# ---------------------------------------------------------------------------

class _MemoryHandler(logging.Handler):
    """Appends formatted log records to the module-level _log_lines list."""

    def emit(self, record: logging.LogRecord) -> None:
        global _log_lines
        entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "message": self.format(record),
        }
        _log_lines.append(entry)
        if len(_log_lines) > _MAX_LOG_LINES:
            _log_lines = _log_lines[-_MAX_LOG_LINES:]


# ---------------------------------------------------------------------------
# Background runner
# ---------------------------------------------------------------------------

def _run_ingestion_thread(run_id: str, force: bool, fund_filters: list[str]) -> None:
    """
    Run PDF ingestion in a background thread.

    Updates _run_state progress after each fund so the status endpoint
    reflects real-time progress.

    fund_filters: exact folder names to process. Empty list = all funds.
    force: when True, ignores manifest hashes for the selected funds only.
    """
    global _run_state, _log_lines

    ingest_logger = logging.getLogger(
        "src.lookthrough.ingestion.ingest_pdf_documents"
    )
    mem_handler = _MemoryHandler()
    mem_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    ingest_logger.addHandler(mem_handler)

    def _log(level: str, msg: str) -> None:
        _log_lines.append({
            "timestamp": datetime.utcnow().isoformat(),
            "level": level,
            "message": msg,
        })

    try:
        base_path = DEFAULT_BASE_FOLDER

        if not base_path.exists():
            raise FileNotFoundError(
                f"Base folder not found: {base_path}. "
                "Is OneDrive synced?"
            )

        ensure_tables()
        _run_migrations()

        manifest = _load_manifest()

        # Build fund work list — filter to exact folder names if specified
        work_list = list(FUND_CONFIG)
        if fund_filters:
            work_list = [c for c in work_list if c["folder"] in fund_filters]
            if not work_list:
                known = [c["folder"] for c in FUND_CONFIG]
                raise ValueError(
                    f"fund_filters {fund_filters} matched no configured folders. "
                    f"Known: {known}"
                )

        # Pre-count total PDF files across all funds
        total_files = 0
        for cfg in work_list:
            folder = base_path / cfg["folder"]
            if folder.exists():
                total_files += len(list(folder.glob("*.pdf")))

        with _state_lock:
            _run_state["progress"] = {
                "total_funds": len(work_list),
                "funds_complete": 0,
                "current_fund": None,
                "total_files": total_files,
                "files_processed": 0,
                "files_skipped": 0,
                "files_new": 0,
                "holdings_extracted": 0,
            }

        _log("INFO", f"Starting ingestion — {len(work_list)} fund(s), {total_files} PDF(s) total")
        if force:
            _log("INFO", "Force re-ingest enabled — all files will be re-processed")

        fund_results: list[dict] = []

        for fund_cfg in work_list:
            folder = base_path / fund_cfg["folder"]
            fund_label = fund_cfg.get("fund_name", fund_cfg["folder"])

            if not folder.exists():
                _log("WARNING", f"Folder not found, skipping: {folder}")
                with _state_lock:
                    _run_state["progress"]["funds_complete"] += 1
                continue

            with _state_lock:
                _run_state["progress"]["current_fund"] = fund_label

            _log("INFO", f"Processing: {fund_label}")

            _, _, _, stats = ingest_fund_folder(
                folder, fund_cfg, db_mode=True, manifest=manifest, force=force
            )

            docs_processed = (
                stats["financial_statements"]
                + stats["lp_statement"]
                + stats["transparency_report"]
            )

            with _state_lock:
                p = _run_state["progress"]
                p["files_processed"] += docs_processed
                p["files_skipped"] += stats["skipped"]
                p["files_new"] += docs_processed
                p["holdings_extracted"] += stats["holdings"]
                p["funds_complete"] += 1

            result = {
                "fund": fund_label,
                "processed": docs_processed,
                "skipped": stats["skipped"],
                "holdings": stats["holdings"],
                "errors": stats["errors"],
            }
            fund_results.append(result)
            _log(
                "INFO" if not stats["errors"] else "WARNING",
                f"  {fund_label}: {docs_processed} processed, "
                f"{stats['skipped']} skipped, "
                f"{stats['holdings']} holdings"
                + (f", {stats['errors']} errors" if stats["errors"] else ""),
            )

        _save_manifest(manifest)

        total_holdings = sum(r["holdings"] for r in fund_results)
        total_new = sum(r["processed"] for r in fund_results)
        _log(
            "INFO",
            f"Ingestion complete — {total_new} files processed, "
            f"{total_holdings} holdings extracted",
        )

        with _state_lock:
            _run_state.update({
                "status": "complete",
                "completed_at": datetime.utcnow().isoformat(),
                "last_result": fund_results,
                "error": None,
            })
            _run_state["progress"]["current_fund"] = None

    except Exception as exc:
        _log("ERROR", f"Ingestion failed: {exc}")
        with _state_lock:
            _run_state.update({
                "status": "failed",
                "completed_at": datetime.utcnow().isoformat(),
                "error": str(exc),
            })

    finally:
        ingest_logger.removeHandler(mem_handler)


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------

class RunRequest(BaseModel):
    force: bool = False
    fund_filters: list[str] = []


# ---------------------------------------------------------------------------
# GET /api/ingestion/manifest
# ---------------------------------------------------------------------------

@router.get("/manifest")
def get_manifest(
    _current_user: User = Depends(get_current_user),
) -> dict:
    """
    Return current manifest state: tracked files, new/changed counts, per-fund breakdown.

    Scans the OneDrive base folder and compares against the ingestion manifest.
    Never raises — returns folder_online=False and an error field on any OS failure.
    """
    base_path = DEFAULT_BASE_FOLDER
    _empty = {
        "total_tracked": 0,
        "by_fund": [],
        "new_files": 0,
        "changed_files": 0,
        "folder_online": False,
        "base_folder": str(base_path),
    }

    try:
        manifest = _load_manifest()
    except Exception as exc:
        return {**_empty, "error": f"Failed to load manifest file: {exc}"}

    # Build a reverse lookup: filename -> hash for "changed" detection
    prev_hash_by_name: dict[str, str] = {
        meta.get("filename", ""): h for h, meta in manifest.items()
    }

    try:
        folder_online = base_path.exists()
    except OSError:
        folder_online = False

    by_fund: list[dict] = []
    total_new = 0
    total_changed = 0
    folder_error: str | None = None

    for cfg in FUND_CONFIG:
        folder = base_path / cfg["folder"]
        fund_name = cfg.get("fund_name", cfg["folder"])

        # Manifest entries for this fund (used when folder is offline)
        fund_manifest_entries = {
            h: meta for h, meta in manifest.items()
            if meta.get("fund") == fund_name
        }
        last_ingested = (
            max((e.get("ingested_at") for e in fund_manifest_entries.values()), default=None)
            if fund_manifest_entries else None
        )

        files: list[dict] = []

        if folder_online and folder.exists():
            try:
                pdf_paths = sorted(folder.glob("*.pdf"))
            except OSError as exc:
                folder_online = False
                folder_error = (
                    f"OneDrive folder not accessible — check that folder is synced "
                    f"({type(exc).__name__}: {exc})"
                )
                pdf_paths = []

            for pdf_path in pdf_paths:
                try:
                    current_hash = _file_hash(pdf_path)
                except OSError:
                    continue

                if current_hash in manifest:
                    status = "ingested"
                    ingested_at = manifest[current_hash].get("ingested_at")
                elif pdf_path.name in prev_hash_by_name:
                    status = "changed"
                    ingested_at = None
                    total_changed += 1
                else:
                    status = "new"
                    ingested_at = None
                    total_new += 1

                files.append({
                    "filename": pdf_path.name,
                    "hash": current_hash[:12],
                    "ingested_at": ingested_at,
                    "status": status,
                })
        else:
            # Folder offline — show last-known manifest entries
            for h, meta in fund_manifest_entries.items():
                files.append({
                    "filename": meta.get("filename", "?"),
                    "hash": h[:12],
                    "ingested_at": meta.get("ingested_at"),
                    "status": "ingested",
                })

        by_fund.append({
            "folder": cfg["folder"],
            "fund_name": fund_name,
            "file_count": len(files),
            "last_ingested": last_ingested,
            "files": files,
        })

    result = {
        "total_tracked": len(manifest),
        "by_fund": by_fund,
        "new_files": total_new,
        "changed_files": total_changed,
        "folder_online": folder_online,
        "base_folder": str(base_path),
    }
    if folder_error:
        result["error"] = folder_error
    return result


# ---------------------------------------------------------------------------
# POST /api/ingestion/run
# ---------------------------------------------------------------------------

@router.post("/run")
def trigger_run(
    body: RunRequest,
    _current_user: User = Depends(get_current_user),
) -> dict:
    """
    Trigger a PDF ingestion run in the background.

    Returns 409 if a run is already in progress.
    Returns immediately with {run_id, status, message}.
    """
    global _run_state, _log_lines

    with _state_lock:
        if _run_state["status"] == "running":
            raise HTTPException(
                status_code=409,
                detail="An ingestion run is already in progress.",
            )

        run_id = str(uuid.uuid4())[:8]
        _log_lines = []  # Clear previous logs

        _run_state = {
            "run_id": run_id,
            "status": "running",
            "started_at": datetime.utcnow().isoformat(),
            "completed_at": None,
            "progress": dict(_IDLE_PROGRESS),
            "error": None,
            "last_result": None,
        }

    thread = threading.Thread(
        target=_run_ingestion_thread,
        args=(run_id, body.force, body.fund_filters),
        daemon=True,
        name=f"ingestion-{run_id}",
    )
    thread.start()

    return {
        "run_id": run_id,
        "status": "started",
        "message": (
            f"Ingestion started (force={body.force}"
            + (f", funds={body.fund_filters}" if body.fund_filters else "")
            + ")"
        ),
    }


# ---------------------------------------------------------------------------
# GET /api/ingestion/status
# ---------------------------------------------------------------------------

@router.get("/status")
def get_status(
    _current_user: User = Depends(get_current_user),
) -> dict:
    """Return current ingestion run state."""
    with _state_lock:
        return dict(_run_state)


# ---------------------------------------------------------------------------
# GET /api/ingestion/logs
# ---------------------------------------------------------------------------

@router.get("/logs")
def get_logs(
    _current_user: User = Depends(get_current_user),
) -> list:
    """Return last 200 log lines from the most recent ingestion run."""
    return list(_log_lines)

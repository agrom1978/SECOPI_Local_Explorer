from fastapi import APIRouter, Query
from ..db import get_conn
from ..settings import get_settings
from ..sync import run_snapshot, run_incremental

router = APIRouter()

@router.post("/run")
def run_sync(mode: str = Query("incremental", pattern="^(snapshot|incremental)$")):
    conn = get_conn()
    try:
        if mode == "snapshot":
            rows = run_snapshot(conn)
        else:
            rows = run_incremental(conn)
        return {"mode": mode, "rows": rows}
    finally:
        conn.close()

@router.get("/status")
def get_status():
    conn = get_conn()
    try:
        s = get_settings()
        row = conn.execute(
            """
            SELECT dataset_id, last_dataset_updated_at, last_run_ts, last_run_status, rows_upserted, last_error
            FROM sync_state
            WHERE dataset_id=?
            """,
            [s.dataset_id],
        ).fetchone()

        if not row:
            return {"dataset_id": s.dataset_id, "status": "MISSING"}

        return {
            "dataset_id": row[0],
            "last_dataset_updated_at": row[1],
            "last_run_ts": row[2],
            "last_run_status": row[3],
            "rows_upserted": row[4],
            "last_error": row[5],
        }
    finally:
        conn.close()

@router.get("/health")
def healthcheck():
    conn = get_conn()
    try:
        s = get_settings()
        conn.execute("SELECT 1").fetchone()
        row = conn.execute(
            """
            SELECT last_run_status, last_run_ts, last_error
            FROM sync_state
            WHERE dataset_id=?
            """,
            [s.dataset_id],
        ).fetchone()

        return {
            "status": "ok",
            "dataset_id": s.dataset_id,
            "last_run_status": row[0] if row else None,
            "last_run_ts": row[1] if row else None,
            "last_error": row[2] if row else None,
        }
    finally:
        conn.close()

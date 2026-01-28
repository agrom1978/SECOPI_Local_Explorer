from fastapi import APIRouter, Query
from ..db import get_conn
from ..settings import get_settings
from ..sync import run_snapshot, run_incremental

router = APIRouter()

@router.post("/run")
def run_sync(mode: str = Query("incremental", pattern="^(snapshot|incremental)$")):
    conn = get_conn()
    if mode == "snapshot":
        rows = run_snapshot(conn)
    else:
        rows = run_incremental(conn)
    return {"mode": mode, "rows": rows}

@router.get("/status")
def get_status():
    conn = get_conn()
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

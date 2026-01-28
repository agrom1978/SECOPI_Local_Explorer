from datetime import datetime
import logging
import time
from typing import Dict, Any, List, Optional
import duckdb
from .socrata import SocrataClient
from .settings import get_settings

logger = logging.getLogger(__name__)

def ensure_sync_state(conn: duckdb.DuckDBPyConnection, dataset_id: str):
    conn.execute("""
        INSERT INTO sync_state(dataset_id, last_dataset_updated_at, last_run_ts, last_run_status, rows_upserted, last_error)
        VALUES (?, NULL, NULL, NULL, 0, NULL)
        ON CONFLICT(dataset_id) DO NOTHING
    """, [dataset_id])

def get_last_dataset_updated_at(conn: duckdb.DuckDBPyConnection, dataset_id: str) -> Optional[datetime]:
    ensure_sync_state(conn, dataset_id)
    row = conn.execute("SELECT last_dataset_updated_at FROM sync_state WHERE dataset_id=?", [dataset_id]).fetchone()
    return row[0] if row else None

def update_sync_state(conn: duckdb.DuckDBPyConnection, dataset_id: str,
                      last_updated_at: Optional[datetime], status: str, rows: int, error: Optional[str]):
    conn.execute("""
        UPDATE sync_state
        SET last_dataset_updated_at=?,
            last_run_ts=NOW(),
            last_run_status=?,
            rows_upserted=?,
            last_error=?
        WHERE dataset_id=?
    """, [last_updated_at, status, rows, error, dataset_id])

def _parse_ts(v: Optional[str]) -> Optional[datetime]:
    if not v:
        return None
    return datetime.fromisoformat(v.replace("Z", "+00:00"))

def _escape_socrata_value(value: str) -> str:
    return value.replace("'", "''")

def upsert_batch(conn: duckdb.DuckDBPyConnection, rows: List[Dict[str, Any]], field_map: Dict[str, str]) -> int:
    if not rows:
        return 0

    cols = list(field_map.keys())
    values = []
    for r in rows:
        row_vals = []
        for c in cols:
            api_name = field_map[c]
            v = r.get(api_name)
            if api_name == ":updated_at":
                v = _parse_ts(v)
            row_vals.append(v)
        values.append(row_vals)

    conn.execute("CREATE TEMP TABLE IF NOT EXISTS stg (" + ", ".join([f"{c} VARCHAR" for c in cols]) + ");")
    conn.execute("DELETE FROM stg;")
    conn.executemany(f"INSERT INTO stg({', '.join(cols)}) VALUES ({', '.join(['?']*len(cols))})", values)

    set_clause = ", ".join([f"{c}=excluded.{c}" for c in cols if c != "uid"])
    select_cols = ", ".join(cols)
    conn.execute(f"""
        WITH ranked AS (
            SELECT {select_cols},
                   ROW_NUMBER() OVER (
                       PARTITION BY uid
                       ORDER BY TRY_CAST(dataset_updated_at AS TIMESTAMP) DESC NULLS LAST
                   ) AS rn
            FROM stg
        )
        INSERT INTO procesos_secop1({select_cols})
        SELECT {select_cols} FROM ranked WHERE rn = 1
        ON CONFLICT(uid) DO UPDATE SET {set_clause}
    """)
    return len(rows)

def _build_field_map(settings) -> Dict[str, str]:
    field_map = dict(settings.fields)
    field_map["dataset_updated_at"] = ":updated_at"
    return field_map

def run_snapshot(conn: duckdb.DuckDBPyConnection) -> int:
    s = get_settings()
    client = SocrataClient(s.socrata_domain, s.socrata_app_token, s.socrata_username, s.socrata_password)

    dataset_id = s.dataset_id
    ensure_sync_state(conn, dataset_id)

    current_year = datetime.now().year
    from_year = current_year - s.default_snapshot_years

    where_clauses = [
        "anno_firma_contrato IS NOT NULL",
        "anno_firma_contrato <> 'Sin Firma'",
        f"anno_firma_contrato >= '{from_year}'",
    ]
    if s.filter_departamento:
        safe_departamento = _escape_socrata_value(s.filter_departamento)
        where_clauses.append(f"departamento_entidad = '{safe_departamento}'")
    if s.filter_municipio:
        safe_municipio = _escape_socrata_value(s.filter_municipio)
        where_clauses.append(f"municipio_entidad = '{safe_municipio}'")
    where = " AND ".join(where_clauses) if where_clauses else None
    order = ":updated_at ASC"

    field_map = _build_field_map(s)

    total = 0
    max_updated = None
    start_time = time.monotonic()

    try:
        logger.info(
            "Starting snapshot sync",
            extra={
                "dataset_id": dataset_id,
                "where": where,
                "order": order,
                "page_limit": s.page_limit,
            },
        )
        for batch in client.iter_query(dataset_id, s.select_str, where, order, s.page_limit):
            logger.debug("Snapshot batch fetched", extra={"dataset_id": dataset_id, "batch_size": len(batch)})
            total += upsert_batch(conn, batch, field_map)
            for r in batch:
                ts = _parse_ts(r.get(":updated_at"))
                if ts and (max_updated is None or ts > max_updated):
                    max_updated = ts

        update_sync_state(conn, dataset_id, max_updated, "SNAPSHOT_OK", total, None)
        logger.info(
            "Snapshot sync completed",
            extra={
                "dataset_id": dataset_id,
                "rows_upserted": total,
                "max_updated": max_updated.isoformat() if max_updated else None,
                "duration_s": round(time.monotonic() - start_time, 2),
            },
        )
        return total
    except Exception as e:
        update_sync_state(conn, dataset_id, max_updated, "SNAPSHOT_ERROR", total, str(e))
        logger.exception(
            "Snapshot sync failed",
            extra={
                "dataset_id": dataset_id,
                "rows_upserted": total,
                "max_updated": max_updated.isoformat() if max_updated else None,
                "duration_s": round(time.monotonic() - start_time, 2),
            },
        )
        raise

def run_incremental(conn: duckdb.DuckDBPyConnection) -> int:
    s = get_settings()
    client = SocrataClient(s.socrata_domain, s.socrata_app_token, s.socrata_username, s.socrata_password)

    dataset_id = s.dataset_id
    ensure_sync_state(conn, dataset_id)

    last = get_last_dataset_updated_at(conn, dataset_id)
    where_clauses = []
    if last:
        safe_last = _escape_socrata_value(last.isoformat())
        where_clauses.append(f":updated_at > '{safe_last}'")
    if s.filter_departamento:
        safe_departamento = _escape_socrata_value(s.filter_departamento)
        where_clauses.append(f"departamento_entidad = '{safe_departamento}'")
    if s.filter_municipio:
        safe_municipio = _escape_socrata_value(s.filter_municipio)
        where_clauses.append(f"municipio_entidad = '{safe_municipio}'")
    where = " AND ".join(where_clauses) if where_clauses else None
    order = ":updated_at ASC"

    field_map = _build_field_map(s)

    total = 0
    max_updated = last
    start_time = time.monotonic()

    try:
        logger.info(
            "Starting incremental sync",
            extra={
                "dataset_id": dataset_id,
                "where": where,
                "order": order,
                "page_limit": s.page_limit,
                "last_updated_at": last.isoformat() if last else None,
            },
        )
        for batch in client.iter_query(dataset_id, s.select_str, where, order, s.page_limit):
            logger.debug("Incremental batch fetched", extra={"dataset_id": dataset_id, "batch_size": len(batch)})
            total += upsert_batch(conn, batch, field_map)
            for r in batch:
                ts = _parse_ts(r.get(":updated_at"))
                if ts and (max_updated is None or ts > max_updated):
                    max_updated = ts
        update_sync_state(conn, dataset_id, max_updated, "INCREMENTAL_OK", total, None)
        logger.info(
            "Incremental sync completed",
            extra={
                "dataset_id": dataset_id,
                "rows_upserted": total,
                "max_updated": max_updated.isoformat() if max_updated else None,
                "duration_s": round(time.monotonic() - start_time, 2),
            },
        )
        return total
    except Exception as e:
        update_sync_state(conn, dataset_id, last, "INCREMENTAL_ERROR", total, str(e))
        logger.exception(
            "Incremental sync failed",
            extra={
                "dataset_id": dataset_id,
                "rows_upserted": total,
                "max_updated": max_updated.isoformat() if max_updated else None,
                "duration_s": round(time.monotonic() - start_time, 2),
            },
        )
        raise

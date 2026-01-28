from __future__ import annotations

import os
import tempfile
from typing import Optional, List

from fastapi import APIRouter, Query, BackgroundTasks, HTTPException
from fastapi.responses import FileResponse
from openpyxl import Workbook
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE

from .db import get_conn
from .settings import get_settings
from . import query as qlib

router = APIRouter()


def _apply_permanent_filters(departamento: Optional[str], municipio: Optional[str]):
    s = get_settings()
    if s.filter_departamento:
        departamento = s.filter_departamento
    if s.filter_municipio:
        municipio = s.filter_municipio
    return departamento, municipio


def _build_where(
    anno: Optional[int],
    anno_min: Optional[int],
    anno_max: Optional[int],
    modalidad: Optional[str],
    destino: Optional[str],
    entidad: Optional[str],
    departamento: Optional[str],
    municipio: Optional[str],
    cuantia_min: Optional[float],
    cuantia_max: Optional[float],
    bpin: Optional[str],
    estado: Optional[str],
    q: Optional[str],
):
    return qlib._build_filters(
        anno,
        anno_min,
        anno_max,
        modalidad,
        destino,
        entidad,
        departamento,
        municipio,
        cuantia_min,
        cuantia_max,
        bpin,
        estado,
        q,
    )


def _parse_cols(cols: Optional[str]) -> Optional[List[str]]:
    if not cols:
        return None
    parts = [c.strip() for c in cols.split(",") if c.strip()]
    if not parts:
        return None
    return parts


def _validate_cols(cols: Optional[List[str]]) -> List[str]:
    if not cols:
        return ["*"]
    # Use catalog from DuckDB to avoid invalid columns
    conn = get_conn()
    available = {r[1] for r in conn.execute("PRAGMA table_info('procesos_secop1')").fetchall()}
    invalid = [c for c in cols if c not in available]
    if invalid:
        raise ValueError(f"Invalid cols: {', '.join(invalid)}")
    return cols


@router.get("/csv")
def export_csv(
    background_tasks: BackgroundTasks,
    anno: Optional[int] = None,
    anno_min: Optional[int] = None,
    anno_max: Optional[int] = None,
    modalidad: Optional[str] = None,
    destino: Optional[str] = None,
    entidad: Optional[str] = None,
    departamento: Optional[str] = None,
    municipio: Optional[str] = None,
    cuantia_min: Optional[float] = None,
    cuantia_max: Optional[float] = None,
    bpin: Optional[str] = None,
    estado: Optional[str] = None,
    q: Optional[str] = None,
    cols: Optional[str] = None,
):
    departamento, municipio = _apply_permanent_filters(departamento, municipio)
    where_clause, params = _build_where(
        anno,
        anno_min,
        anno_max,
        modalidad,
        destino,
        entidad,
        departamento,
        municipio,
        cuantia_min,
        cuantia_max,
        bpin,
        estado,
        q,
    )

    conn = get_conn()
    try:
        sel_cols = _validate_cols(_parse_cols(cols))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        path = tmp.name

    select_list = "*" if sel_cols == ["*"] else ", ".join(sel_cols)
    sql = f"SELECT {select_list} FROM procesos_secop1 {where_clause} ORDER BY dataset_updated_at DESC"
    conn.execute(f"COPY ({sql}) TO '{path}' (HEADER, DELIMITER ',')", params)
    background_tasks.add_task(os.remove, path)
    return FileResponse(path, filename="procesos_export.csv", media_type="text/csv", background=background_tasks)


@router.get("/xlsx")
def export_xlsx(
    background_tasks: BackgroundTasks,
    anno: Optional[int] = None,
    anno_min: Optional[int] = None,
    anno_max: Optional[int] = None,
    modalidad: Optional[str] = None,
    destino: Optional[str] = None,
    entidad: Optional[str] = None,
    departamento: Optional[str] = None,
    municipio: Optional[str] = None,
    cuantia_min: Optional[float] = None,
    cuantia_max: Optional[float] = None,
    bpin: Optional[str] = None,
    estado: Optional[str] = None,
    q: Optional[str] = None,
    limit: int = Query(20000, ge=1, le=200000),
    cols: Optional[str] = None,
):
    departamento, municipio = _apply_permanent_filters(departamento, municipio)
    where_clause, params = _build_where(
        anno,
        anno_min,
        anno_max,
        modalidad,
        destino,
        entidad,
        departamento,
        municipio,
        cuantia_min,
        cuantia_max,
        bpin,
        estado,
        q,
    )

    conn = get_conn()
    try:
        sel_cols = _validate_cols(_parse_cols(cols))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    select_list = "*" if sel_cols == ["*"] else ", ".join(sel_cols)
    sql = f"SELECT {select_list} FROM procesos_secop1 {where_clause} ORDER BY dataset_updated_at DESC LIMIT ?"
    params = params + [limit]
    cur = conn.execute(sql, params)
    rows = cur.fetchall()
    cols = [c[0] for c in cur.description]

    wb = Workbook()
    ws = wb.active
    ws.title = "procesos"
    ws.append(cols)
    for row in rows:
        cleaned = []
        for val in row:
            if isinstance(val, str):
                cleaned.append(ILLEGAL_CHARACTERS_RE.sub("", val))
            else:
                cleaned.append(val)
        ws.append(cleaned)

    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        path = tmp.name
    wb.save(path)
    background_tasks.add_task(os.remove, path)
    return FileResponse(
        path,
        filename="procesos_export.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        background=background_tasks,
    )

from typing import Optional
from fastapi import APIRouter, Query
from ..db import get_conn
from .. import query as qlib

router = APIRouter()

@router.get("/procesos")
def get_procesos(
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
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    conn = get_conn()
    try:
        from ..settings import get_settings
        s = get_settings()
        entidad_exact = False
        if s.filter_entidad:
            entidad = s.filter_entidad
            entidad_exact = True
        if s.filter_departamento:
            departamento = s.filter_departamento
        if s.filter_municipio:
            municipio = s.filter_municipio
        items = qlib.list_procesos(
            conn,
            anno,
            anno_min,
            anno_max,
            modalidad,
            destino,
            entidad,
            entidad_exact,
            departamento,
            municipio,
            cuantia_min,
            cuantia_max,
            bpin,
            estado,
            q,
            limit,
            offset,
        )
        total = qlib.count_procesos(
            conn,
            anno,
            anno_min,
            anno_max,
            modalidad,
            destino,
            entidad,
            entidad_exact,
            departamento,
            municipio,
            cuantia_min,
            cuantia_max,
            bpin,
            estado,
            q,
        )
        return {"total": total, "limit": limit, "offset": offset, "items": items}
    finally:
        conn.close()

@router.get("/catalogos/{catalogo}")
def get_catalogo(
    catalogo: str,
    q: Optional[str] = None,
    limit: int = Query(200, ge=1, le=2000),
):
    conn = get_conn()
    try:
        from ..settings import get_settings
        s = get_settings()
        values = qlib.list_catalog(
            conn,
            catalogo,
            limit,
            q,
            s.filter_departamento,
            s.filter_municipio,
        )
        return {"catalogo": catalogo, "items": values}
    finally:
        conn.close()

@router.get("/stats/resumen")
def get_stats(
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
):
    conn = get_conn()
    try:
        from ..settings import get_settings
        s = get_settings()
        entidad_exact = False
        if s.filter_entidad:
            entidad = s.filter_entidad
            entidad_exact = True
        if s.filter_departamento:
            departamento = s.filter_departamento
        if s.filter_municipio:
            municipio = s.filter_municipio
        return qlib.get_stats(
            conn,
            anno,
            anno_min,
            anno_max,
            modalidad,
            destino,
            entidad,
            entidad_exact,
            departamento,
            municipio,
            cuantia_min,
            cuantia_max,
            bpin,
            estado,
            q,
        )
    finally:
        conn.close()

from typing import Any, Dict, List, Optional, Tuple
import duckdb

ALLOWED_CATALOG_COLUMNS = {
    "anno_firma_contrato",
    "modalidad_de_contratacion",
    "destino_gasto",
    "nombre_entidad",
    "departamento_entidad",
    "municipio_entidad",
    "estado_del_proceso",
    "codigo_bpin",
}

SELECT_COLUMNS = [
    "uid",
    "anno_firma_contrato",
    "anno_cargue_secop",
    "modalidad_de_contratacion",
    "destino_gasto",
    "estado_del_proceso",
    "nombre_entidad",
    "departamento_entidad",
    "municipio_entidad",
    "cuantia_contrato",
    "cuantia_proceso",
    "codigo_bpin",
    "ultima_actualizacion",
    "dataset_updated_at",
]

def _build_filters(
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
) -> Tuple[str, List[Any]]:
    clauses = []
    params: List[Any] = []

    if anno is not None:
        clauses.append("TRY_CAST(anno_firma_contrato AS INTEGER) = ?")
        params.append(anno)
    if anno_min is not None:
        clauses.append("TRY_CAST(anno_firma_contrato AS INTEGER) >= ?")
        params.append(anno_min)
    if anno_max is not None:
        clauses.append("TRY_CAST(anno_firma_contrato AS INTEGER) <= ?")
        params.append(anno_max)
    if modalidad:
        clauses.append("modalidad_de_contratacion = ?")
        params.append(modalidad)
    if destino:
        clauses.append("destino_gasto = ?")
        params.append(destino)
    if entidad:
        clauses.append("nombre_entidad ILIKE ?")
        params.append(f"%{entidad}%")
    _add_case_insensitive_filter(clauses, params, "departamento_entidad", departamento)
    _add_case_insensitive_filter(clauses, params, "municipio_entidad", municipio)
    if cuantia_min is not None:
        clauses.append("cuantia_contrato >= ?")
        params.append(cuantia_min)
    if cuantia_max is not None:
        clauses.append("cuantia_contrato <= ?")
        params.append(cuantia_max)
    if bpin:
        clauses.append("codigo_bpin = ?")
        params.append(bpin)
    if estado:
        clauses.append("estado_del_proceso = ?")
        params.append(estado)
    if q:
        clauses.append("(nombre_entidad ILIKE ? OR municipio_entidad ILIKE ? OR departamento_entidad ILIKE ?)")
        like = f"%{q}%"
        params.extend([like, like, like])

    if not clauses:
        return "", params

    return "WHERE " + " AND ".join(clauses), params


def _add_case_insensitive_filter(
    clauses: List[str],
    params: List[Any],
    column: str,
    value: Optional[str],
) -> None:
    if value:
        clauses.append(f"UPPER({column}) = UPPER(?)")
        params.append(value)


def _rows_to_dicts(cursor: duckdb.DuckDBPyConnection, rows: List[tuple]) -> List[Dict[str, Any]]:
    cols = [c[0] for c in cursor.description]
    return [dict(zip(cols, row)) for row in rows]


def list_procesos(
    conn: duckdb.DuckDBPyConnection,
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
    limit: int,
    offset: int,
) -> List[Dict[str, Any]]:
    where_clause, params = _build_filters(
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

    sql = f"SELECT {', '.join(SELECT_COLUMNS)} FROM procesos_secop1 {where_clause} ORDER BY dataset_updated_at DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    cur = conn.execute(sql, params)
    rows = cur.fetchall()
    return _rows_to_dicts(cur, rows)


def count_procesos(
    conn: duckdb.DuckDBPyConnection,
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
) -> int:
    where_clause, params = _build_filters(
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

    sql = f"SELECT COUNT(*) FROM procesos_secop1 {where_clause}"
    row = conn.execute(sql, params).fetchone()
    return int(row[0]) if row else 0


def list_catalog(
    conn: duckdb.DuckDBPyConnection,
    column: str,
    limit: int,
    q: Optional[str],
    departamento: Optional[str],
    municipio: Optional[str],
) -> List[Any]:
    if column not in ALLOWED_CATALOG_COLUMNS:
        raise ValueError("Invalid catalog column")

    params: List[Any] = []
    sql = f"SELECT DISTINCT {column} AS value FROM procesos_secop1 WHERE {column} IS NOT NULL"

    if q:
        sql += f" AND CAST({column} AS VARCHAR) ILIKE ?"
        params.append(f"%{q}%")
    extra_clauses: List[str] = []
    _add_case_insensitive_filter(extra_clauses, params, "departamento_entidad", departamento)
    _add_case_insensitive_filter(extra_clauses, params, "municipio_entidad", municipio)
    if extra_clauses:
        sql += " AND " + " AND ".join(extra_clauses)

    sql += " ORDER BY value LIMIT ?"
    params.append(limit)

    rows = conn.execute(sql, params).fetchall()
    return [r[0] for r in rows]


def get_stats(
    conn: duckdb.DuckDBPyConnection,
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
) -> Dict[str, Any]:
    where_clause, params = _build_filters(
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

    sql = f"""
        SELECT
            COUNT(*) AS total,
            SUM(cuantia_contrato) AS total_cuantia_contrato,
            SUM(cuantia_proceso) AS total_cuantia_proceso,
            MIN(anno_firma_contrato) AS min_anno_firma_contrato,
            MAX(anno_firma_contrato) AS max_anno_firma_contrato
        FROM procesos_secop1 {where_clause}
    """

    row = conn.execute(sql, params).fetchone()
    if not row:
        return {"total": 0}

    return {
        "total": int(row[0]) if row[0] is not None else 0,
        "total_cuantia_contrato": float(row[1]) if row[1] is not None else 0.0,
        "total_cuantia_proceso": float(row[2]) if row[2] is not None else 0.0,
        "min_anno_firma_contrato": int(row[3]) if row[3] is not None else None,
        "max_anno_firma_contrato": int(row[4]) if row[4] is not None else None,
    }

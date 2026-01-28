SECOP I Local Explorer
======================

Aplicación local para explorar el dataset SECOP I con réplica incremental y filtros permanentes.

Funcionalidades
---------------
- Snapshot inicial (filtrado por departamento/municipio).
- Incremental diario por `:updated_at`.
- API con filtros y paginación.
- Exportación CSV/XLSX (con columnas opcionales).
- UI web ligera con filtros, stats y export.

Requisitos
----------
- Python 3.13 o 3.12
- Windows (scripts .bat)

Configuración rápida
--------------------
1) Crear `.env` desde `.env.example`
2) Ajustar filtros permanentes:

```
FILTER_DEPARTAMENTO=LA GUAJIRA
FILTER_MUNICIPIO=ALBANIA
```

3) Instalar dependencias y correr la app:

```
cd secop1_local_explorer
python -m venv .venv
.venv\Scripts\pip install -r requirements.txt
scripts\run_app.bat
```

API
---
- `GET /procesos`
- `GET /catalogos/{catalogo}`
- `GET /stats/resumen`
- `POST /sync/run?mode=snapshot|incremental`
- `GET /sync/status`
- `GET /export/csv`
- `GET /export/xlsx`

UI
--
Abrir en el navegador: `http://127.0.0.1:8001/`

Nota sobre export
-----------------
`/export/xlsx` limpia caracteres ilegales para Excel.

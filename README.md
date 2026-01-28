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
- `GET /sync/health`
- `GET /export/csv`
- `GET /export/xlsx`

UI
--
Abrir en el navegador: `http://127.0.0.1:8001/`

Nota sobre export
-----------------
`/export/xlsx` limpia caracteres ilegales para Excel.

Testing
-------
Ejecutar pruebas unitarias:

```
python -m unittest discover -s tests
```

Healthcheck
-----------
Validar conectividad a DuckDB y estado básico de sincronización:

```
GET /sync/health
```

Mejoras futuras (backlog)
-------------------------
1) Ampliar `/sync/health` con validaciones adicionales (por ejemplo, antigüedad del último sync).
2) Documentar todas las variables de entorno en una tabla de configuración.
3) Agregar pruebas unitarias para `_build_filters`, paginación y rangos de cuantía/anno.
4) Extraer validaciones de filtros a un módulo reutilizable.
5) Centralizar configuración de logging (formato, nivel, destino) para producción.
6) Añadir caché con TTL para catálogos frecuentes.
7) Agregar nuevos endpoints de estadísticas por modalidad/estado/entidad.
8) Documentar ejemplos de parámetros para exportaciones CSV/XLSX.

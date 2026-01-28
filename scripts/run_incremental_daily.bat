@echo off
setlocal
cd /d %~dp0\..

if not exist .venv (
  echo Missing .venv. Create venv first.
  exit /b 1
)

call .venv\Scripts\activate

REM Run incremental sync
python -c "from app.db import get_conn; from app.sync import run_incremental; print(run_incremental(get_conn()))"

endlocal

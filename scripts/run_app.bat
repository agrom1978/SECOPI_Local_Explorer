@echo off
setlocal
cd /d %~dp0\..

if not exist .venv (
  echo Missing .venv. Create venv first.
  exit /b 1
)

call .venv\Scripts\activate

start "" http://127.0.0.1:8001/
python -m uvicorn app.main:app --host 127.0.0.1 --port 8001 --reload

endlocal

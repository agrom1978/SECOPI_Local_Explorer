from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from .routers import procesos, sync
from . import exports

app = FastAPI(title="SECOP I Local Explorer", version="0.1.0")
app.include_router(procesos.router, tags=["Procesos"])
app.include_router(sync.router, prefix="/sync", tags=["Sync"])
app.include_router(exports.router, prefix="/export", tags=["Export"])

app.mount("/static", StaticFiles(directory="app/static"), name="static")


@app.get("/")
def index():
    index_path = Path("app/static/index.html").resolve()
    return FileResponse(
        str(index_path),
        headers={
            "Cache-Control": "no-store",
            "X-UI-Path": str(index_path),
            "X-UI-Version": "20260129",
        },
    )

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from .routers import procesos, sync
from . import exports

app = FastAPI(title="SECOP I Local Explorer", version="0.1.0")
app.include_router(procesos.router, tags=["Procesos"])
app.include_router(sync.router, prefix="/sync", tags=["Sync"])
app.include_router(exports.router, prefix="/export", tags=["Export"])

app.mount("/static", StaticFiles(directory="app/static"), name="static")


@app.get("/")
def index():
    return FileResponse("app/static/index.html")

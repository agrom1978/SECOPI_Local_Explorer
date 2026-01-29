import os
from dataclasses import dataclass
import yaml
from dotenv import load_dotenv

load_dotenv()

@dataclass
class Settings:
    socrata_domain: str
    dataset_id: str
    socrata_app_token: str | None
    socrata_username: str | None
    socrata_password: str | None
    duckdb_path: str
    default_snapshot_years: int
    page_limit: int
    filter_departamento: str | None
    filter_municipio: str | None
    filter_entidad: str | None
    primary_key: str
    fields: dict
    system_fields: dict
    select: list
    select_str: str
    export_exclude: list

_SETTINGS: Settings | None = None

def get_settings() -> Settings:
    global _SETTINGS
    if _SETTINGS:
        return _SETTINGS

    socrata_domain = os.getenv("SOCRATA_DOMAIN", "www.datos.gov.co")
    dataset_id = os.getenv("DATASET_ID", "f789-7hwg")
    app_token = os.getenv("SOCRATA_APP_TOKEN") or None
    user = os.getenv("SOCRATA_USERNAME") or None
    pwd = os.getenv("SOCRATA_PASSWORD") or None

    duckdb_path = os.getenv("DUCKDB_PATH", "./data/secop1.duckdb")
    default_snapshot_years = int(os.getenv("DEFAULT_SNAPSHOT_YEARS", "5"))
    page_limit = int(os.getenv("PAGE_LIMIT", "50000"))
    filter_departamento = os.getenv("FILTER_DEPARTAMENTO") or None
    filter_municipio = os.getenv("FILTER_MUNICIPIO") or None
    filter_entidad = os.getenv("FILTER_ENTIDAD", "LA GUAJIRA - ALCALDiA MUNICIPIO DE ALBANIA") or None

    with open("./config/dataset.yml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    select = cfg["select"]
    select_str = ",".join(select)
    export_exclude = cfg.get("export_exclude", []) or []

    _SETTINGS = Settings(
        socrata_domain=socrata_domain,
        dataset_id=dataset_id,
        socrata_app_token=app_token,
        socrata_username=user,
        socrata_password=pwd,
        duckdb_path=duckdb_path,
        default_snapshot_years=default_snapshot_years,
        page_limit=page_limit,
        filter_departamento=filter_departamento,
        filter_municipio=filter_municipio,
        filter_entidad=filter_entidad,
        primary_key=cfg["primary_key"],
        fields=cfg["fields"],
        system_fields=cfg["system_fields"],
        select=select,
        select_str=select_str,
        export_exclude=export_exclude,
    )
    return _SETTINGS

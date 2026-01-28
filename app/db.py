import duckdb
from .settings import get_settings

def init_db(conn: duckdb.DuckDBPyConnection):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS procesos_secop1 (
      uid TEXT PRIMARY KEY,
      anno_cargue_secop DOUBLE,
      anno_firma_contrato TEXT,
      nivel_entidad TEXT,
      orden_entidad TEXT,
      nombre_entidad TEXT,
      nit_de_la_entidad TEXT,
      c_digo_de_la_entidad DOUBLE,
      id_modalidad DOUBLE,
      modalidad_de_contratacion TEXT,
      estado_del_proceso TEXT,
      causal_de_otras_formas_de TEXT,
      id_regimen_de_contratacion DOUBLE,
      nombre_regimen_de_contratacion TEXT,
      id_objeto_a_contratar DOUBLE,
      objeto_a_contratar TEXT,
      detalle_del_objeto_a_contratar TEXT,
      tipo_de_contrato TEXT,
      municipio_de_obtencion TEXT,
      municipio_de_entrega TEXT,
      municipios_ejecucion TEXT,
      fecha_de_cargue_en_el_secop TIMESTAMP,
      numero_de_constancia TEXT,
      numero_de_proceso TEXT,
      numero_de_contrato TEXT,
      cuantia_proceso DOUBLE,
      id_grupo TEXT,
      nombre_grupo TEXT,
      id_familia TEXT,
      nombre_familia TEXT,
      id_clase TEXT,
      nombre_clase TEXT,
      id_adjudicacion TEXT,
      tipo_identifi_del_contratista TEXT,
      identificacion_del_contratista TEXT,
      nom_razon_social_contratista TEXT,
      dpto_y_muni_contratista TEXT,
      tipo_doc_representante_legal TEXT,
      identific_representante_legal TEXT,
      nombre_del_represen_legal TEXT,
      fecha_de_firma_del_contrato TIMESTAMP,
      fecha_ini_ejec_contrato TIMESTAMP,
      plazo_de_ejec_del_contrato DOUBLE,
      rango_de_ejec_del_contrato TEXT,
      tiempo_adiciones_en_dias DOUBLE,
      tiempo_adiciones_en_meses DOUBLE,
      fecha_fin_ejec_contrato TIMESTAMP,
      compromiso_presupuestal TEXT,
      cuantia_contrato DOUBLE,
      valor_total_de_adiciones DOUBLE,
      valor_contrato_con_adiciones DOUBLE,
      objeto_del_contrato_a_la TEXT,
      proponentes_seleccionados TEXT,
      calificacion_definitiva TEXT,
      id_sub_unidad_ejecutora TEXT,
      nombre_sub_unidad_ejecutora TEXT,
      ruta_proceso_en_secop_i TEXT,
      moneda TEXT,
      es_postconflicto DOUBLE,
      marcacion_adiciones DOUBLE,
      posicion_rubro TEXT,
      nombre_rubro TEXT,
      valor_rubro DOUBLE,
      sexo_replegal TEXT,
      pilar_acuerdo_paz TEXT,
      punto_acuerdo_paz TEXT,
      municipio_entidad TEXT,
      departamento_entidad TEXT,
      ultima_actualizacion TIMESTAMP,
      fecha_liquidacion TIMESTAMP,
      cumpledecreto248 TEXT,
      incluyebienesdecreto248 TEXT,
      cumple_sentencia_t302 TEXT,
      es_mipyme TEXT,
      tama_o_mipyme TEXT,
      codigo_bpin TEXT,
      destino_gasto TEXT,
      pliegos_tipo TEXT,
      sector_pliegos_tipo TEXT,
      dataset_updated_at TIMESTAMP
    );
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS sync_state (
      dataset_id TEXT PRIMARY KEY,
      last_dataset_updated_at TIMESTAMP,
      last_run_ts TIMESTAMP,
      last_run_status TEXT,
      rows_upserted INTEGER,
      last_error TEXT
    );
    """)

def get_conn() -> duckdb.DuckDBPyConnection:
    s = get_settings()
    conn = duckdb.connect(s.duckdb_path)
    init_db(conn)
    return conn

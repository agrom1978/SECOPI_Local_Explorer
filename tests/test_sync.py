import unittest

import duckdb

from app import query as qlib
from app import sync as sync_lib


class TestSocrataEscaping(unittest.TestCase):
    def test_escape_socrata_value_doubles_single_quotes(self):
        value = "O'Connor"
        escaped = sync_lib._escape_socrata_value(value)
        self.assertEqual(escaped, "O''Connor")


class TestCatalogFilters(unittest.TestCase):
    def setUp(self):
        self.conn = duckdb.connect(":memory:")
        self.conn.execute(
            """
            CREATE TABLE procesos_secop1 (
                nombre_entidad TEXT,
                departamento_entidad TEXT,
                municipio_entidad TEXT
            )
            """
        )
        self.conn.execute(
            """
            INSERT INTO procesos_secop1(nombre_entidad, departamento_entidad, municipio_entidad)
            VALUES
                ('Entidad A', 'Antioquia', 'Medellin'),
                ('Entidad B', 'ANTIOQUIA', 'MEDELLIN'),
                ('Entidad C', 'Cundinamarca', 'Bogota')
            """
        )

    def tearDown(self):
        self.conn.close()

    def test_list_catalog_case_insensitive_filters(self):
        values = qlib.list_catalog(
            self.conn,
            "nombre_entidad",
            limit=10,
            q=None,
            departamento="antioquia",
            municipio="medellin",
        )
        self.assertEqual(set(values), {"Entidad A", "Entidad B"})

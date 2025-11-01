import unittest
from metabase_create_admin import wait_for_metabase, create_admin
from metabase_connect_clickhouse import wait_for_metabase as wait_mb, login_admin, create_clickhouse_db

class TestMetabaseAutomation(unittest.TestCase):
    def test_wait_for_metabase(self):
        # Debe devolver True si Metabase está corriendo
        self.assertIsInstance(wait_for_metabase(), bool)

    def test_create_admin(self):
        # Solo valida que la función se ejecuta (no el resultado real)
        try:
            create_admin()
        except Exception as e:
            self.fail(f"create_admin() lanzó excepción: {e}")

    def test_login_admin(self):
        # Debe devolver None o un string (session id)
        result = login_admin()
        self.assertTrue(result is None or isinstance(result, str))

    def test_create_clickhouse_db(self):
        # Solo valida que la función se ejecuta (no el resultado real)
        try:
            create_clickhouse_db('dummy_session_id')
        except Exception as e:
            self.fail(f"create_clickhouse_db() lanzó excepción: {e}")

if __name__ == "__main__":
    unittest.main()

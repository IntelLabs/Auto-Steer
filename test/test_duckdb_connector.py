"""Test AutoSteer-G DuckDB connector"""
from connectors.duckdb_connector import DuckDBConnector
import unittest


class TestDuckDBConnector(unittest.TestCase):
    """TestCase for AutoSteer-G's DuckDB connector"""

    def setUp(self) -> None:
        self.connector = DuckDBConnector()
        self.connector.connect()

    def tearDown(self) -> None:
        self.connector.close()

    def test_connection(self):
        self.assertIsNotNone(self.connector.connection)

    def test_explain(self):
        with open('./data/expected_duckdb_plan.txt', 'r', encoding='utf-8') as f:
            expected_plan = ''.join(f.readlines())
        result = self.connector.explain('SELECT 42;')
        self.assertEqual(''.join(result.split()), ''.join(expected_plan.split()))

    def test_execution(self):
        result = self.connector.execute('SELECT 42;')
        self.assertEqual(result.result, '[(42,)]')
        self.assertGreater(result.time_usecs, 0)

    def test_disable_knobs(self):
        knobs = DuckDBConnector.get_knobs()
        self.connector.set_disabled_knobs(knobs)

    def test_num_knobs(self):
        self.assertEqual(len(DuckDBConnector.get_knobs()), 14)


if __name__ == '__main__':
    unittest.main()

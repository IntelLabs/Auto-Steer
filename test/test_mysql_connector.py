"""Test AutoSteer-G MySQL connector"""
from connectors.mysql_connector import MySqlConnector
import unittest
import json


class TestMySQLConnector(unittest.TestCase):
    """TestCase for AutoSteer-G's MySQL connector"""

    def setUp(self) -> None:
        self.connector = MySqlConnector()
        self.connector.connect()

    def tearDown(self) -> None:
        self.connector.close()

    def test_connection(self):
        self.assertIsNotNone(self.connector.connection)

    def test_explain(self):
        with open('./data/expected_mysql_plan.json', 'r', encoding='utf-8') as f:
            expected_plan = ''.join(f.readlines())
        actual_plan = self.connector.explain('SELECT 42;')
        self.assertEqual(''.join(actual_plan.split()), ''.join(expected_plan.split()))

    def test_execution(self):
        result = self.connector.execute('SELECT 42;')
        self.assertEqual(result.result[0][0], 42)
        self.assertGreater(result.time_usecs, 0)

    def test_disable_knobs(self):
        knobs = MySqlConnector.get_knobs()
        self.assertTrue(all(self.connector.get_knob(knob) for knob in knobs))
        self.connector.set_disabled_knobs(knobs)
        self.assertTrue(not any(self.connector.get_knob(knob) for knob in knobs))

    def test_num_knobs(self):
        self.assertEqual(len(MySqlConnector.get_knobs()), 22)


if __name__ == '__main__':
    unittest.main()

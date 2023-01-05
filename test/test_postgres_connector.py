# Copyright 2022 Intel Corporation
# SPDX-License-Identifier: MIT
#
"""Test AutoSteer-G PostgreSQL connector"""
from connectors.postgres_connector import PostgresConnector
import unittest


class TestPostgresConnector(unittest.TestCase):
    """TestCase for PostgreSQL connector"""

    def setUp(self) -> None:
        self.connector = PostgresConnector()
        self.connector.connect()

    def tearDown(self) -> None:
        self.connector.close()

    def test_connection(self):
        self.assertIsNotNone(self.connector.connection)

    def test_explain(self):
        with open('./data/expected_postgres_plan.txt', 'r', encoding='utf-8') as f:
            expected_plan = ''.join(f.readlines())
        actual_plan = self.connector.explain('SELECT 42;')
        self.assertEqual(''.join(actual_plan.split()), ''.join(expected_plan.split()))

    def test_execution(self):
        result = self.connector.execute('SELECT 42;')
        self.assertEqual(result.result[0][0], 42)
        self.assertGreater(result.time_usecs, 0)

    def test_disable_knobs(self):
        knobs = PostgresConnector.get_knobs()
        self.assertTrue(all(self.connector.get_knob(knob) for knob in knobs))
        self.connector.set_disabled_knobs(knobs)
        self.assertTrue(not any(self.connector.get_knob(knob) for knob in knobs))

    def test_num_knobs(self):
        self.assertEqual(len(PostgresConnector.get_knobs()), 20)


if __name__ == '__main__':
    unittest.main()

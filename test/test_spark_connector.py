"""Test AutoSteer-G SparkSQL connector"""
from connectors.spark_connector import SparkConnector
import unittest


class TestSparkConnector(unittest.TestCase):
    """TestCase for SparkSQL connector"""

    def setUp(self) -> None:
        self.connector = SparkConnector()
        self.connector.connect()

    def tearDown(self) -> None:
        self.connector.close()

    def test_explain(self):
        result = self.connector.explain('SELECT 42;')
        self.assertTrue(result.startswith('== Physical Plan =='))

    def test_execution(self):
        result = self.connector.execute('SELECT 42;')
        self.assertEqual(result.result[0], 42)
        self.assertGreater(result.time_usecs, 0)

    def test_disable_knobs(self):
        knobs = SparkConnector.get_knobs()
        self.connector.set_disabled_knobs([])
        # test that all knobs are turned on
        self.assertTrue(all(self.connector.get_knob(knob) for knob in knobs))
        self.connector.set_disabled_knobs(knobs)
        # test that all knobs are turned off
        self.assertTrue(not any(self.connector.get_knob(knob) for knob in knobs))
        # turn on all knobs
        self.connector.set_disabled_knobs([])
        self.assertTrue(all(self.connector.get_knob(knob) for knob in knobs))

    def test_random_knobs(self):
        knobs = SparkConnector.get_knobs()
        self.assertTrue(all(self.connector.get_knob(knob) for knob in knobs))
        random_knob_idx = 5
        self.connector.set_disabled_knobs(knobs)
        self.assertTrue(not any(self.connector.get_knob(knob) for knob in knobs))
        self.connector.set_disabled_knobs(knobs[random_knob_idx:random_knob_idx + 1])
        for idx, knob in enumerate(knobs):
            if idx == random_knob_idx:
                self.assertFalse(self.connector.get_knob(knob))
            else:
                self.assertTrue(self.connector.get_knob(knob))

    def test_num_knobs(self):
        self.assertEqual(len(SparkConnector.get_knobs()), 49)


if __name__ == '__main__':
    unittest.main()

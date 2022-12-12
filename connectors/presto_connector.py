"""This module provides several helper functions to connect to presto server"""
from typing import Type
import prestodb
import connectors.connector
from connectors.connector import DBConnector
import configparser
import os
from inference.preprocessing.preprocess_presto_plans import PrestoPlanPreprocessor
from inference.preprocessing.preprocessor import QueryPlanPreprocessor


class PrestoConnector(DBConnector):
    """This class wraps a Presto session"""

    def __init__(self):
        super().__init__()
        self.config = configparser.ConfigParser()
        self.config.read(os.path.dirname(__file__) + '/../configs/presto.cfg')
        self.session_properties = {}
        self.connect()

    def connect(self):
        defaults = self.config['DEFAULT']
        self.session_properties['query_max_execution_time'] = defaults['execution_timeout']
        self.connection: prestodb.dbapi.Connection = prestodb.dbapi.connect(
            host=defaults['host'],
            port=defaults['port'],
            user=defaults['user'],
            catalog=defaults['catalog'],
            schema=defaults['schema'],
            request_timeout=prestodb.constants.DEFAULT_REQUEST_TIMEOUT,
            session_properties=self.session_properties,
        )

    def close(self):
        self.connection.close()

    def execute(self, query) -> connectors.connector.DBConnector.TimedResult:
        cur = self.connection.cursor()
        cur.execute(query)
        result = cur.fetchall()
        return connectors.connector.DBConnector.TimedResult(result, cur.stats['elapsedTimeMillis'] * 1_000)

    def set_disabled_knobs(self, knobs: list) -> None:
        all_knobs = PrestoConnector.get_knobs()
        for knob in all_knobs:
            self.connection.session_properties[knob] = True
        for knob in knobs:
            self.connection.session_properties[knob] = False

    def explain(self, query):
        # fragmented_query_plan, _ = self.execute('EXPLAIN (TYPE DISTRIBUTED, FORMAT JSON) ' + query)
        timed_result = self.execute('EXPLAIN (FORMAT JSON) ' + query)
        return timed_result.result[0][0]

    def _get_connection(self) -> prestodb.dbapi.Connection:
        return self.connection

    def set_catalog(self, catalog):
        self.connection.catalog = catalog

    def set_schema(self, schema):
        self.connection.schema = schema

    def get_knob(self, knob: str) -> bool:
        return self.session_properties[knob]

    @staticmethod
    def get_plan_preprocessor() -> Type[QueryPlanPreprocessor]:
        """Return the type of the query plan preprocessor"""
        return PrestoPlanPreprocessor

    @staticmethod
    def get_name() -> str:
        return 'presto'

    @staticmethod
    def get_knobs() -> list:
        """Static method returning all knobs defined for this connector"""
        with open(os.path.dirname(__file__) + '/../knobs/presto_top_7.txt', 'r', encoding='utf-8') as f:
            return [line.replace('\n', '') for line in f.readlines()]

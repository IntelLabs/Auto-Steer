"""This module provides a connection to the PostgreSQL database for benchmarking"""
import time
from connectors.connector import DBConnector
import duckdb
import configparser
import os


class DuckDBConnector(DBConnector):
    """This class handles the connection to the benchmarked PostgreSQL database"""

    def __init__(self):
        super().__init__()
        self.config = configparser.ConfigParser()
        self.config.read(os.path.dirname(__file__) + '/../configs/duckdb.cfg')
        self.connection = None
        self.connect()

    def connect(self):
        defaults = self.config['DEFAULT']
        self.connection = duckdb.connect(defaults['DATABASE'])
        self.connection.execute(f'PRAGMA memory_limit=\'{defaults["MEMORY_LIMIT"]}\';')
        self.connection.execute(f'PRAGMA threads={defaults["THREADS"]}')

    def close(self):
        self.connection.close()

    def execute(self, query) -> DBConnector.TimedResult:
        begin = time.time_ns()
        result = self.connection.execute(query).fetchall()
        elapsed_time_usecs = int((time.time_ns() - begin) / 1_000)
        return DBConnector.TimedResult(str(result), elapsed_time_usecs)

    def explain(self, query):
        result = self.connection.execute(f'EXPLAIN {query}').fetchone()  # tuple('physical plan', <actual plan>)
        return result[1]  # return the actual plan

    def get_knob(self, knob: str) -> bool:
        """Get current status of a knob"""
        raise Exception('Getting the current status of a rule is not supported by DuckDB')

    def set_disabled_knobs(self, knobs: list) -> None:
        """Toggle a list of knobs"""
        self.connection.execute('PRAGMA disabled_optimizers = \'\';')  # reset config
        self.connection.execute(f'PRAGMA disabled_optimizers = \'{",".join(knobs)}\';')

    @staticmethod
    def get_name() -> str:
        return "duckdb"

    @staticmethod
    def get_knobs() -> list:
        """Static method returning all knobs defined for this connector"""
        with open(os.path.dirname(__file__) + '/../knobs/duckdb.txt', 'r', encoding='utf-8') as f:
            return [line.replace('\n', '') for line in f.readlines()]

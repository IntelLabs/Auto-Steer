"""This module provides a connection to the PostgreSQL database for benchmarking"""
import psycopg2
from connectors.connector import DBConnector
import configparser
import time
import os
import json


class PostgresConnector(DBConnector):
    """This class handles the connection to the tested PostgreSQL database"""

    def __init__(self):
        super().__init__()
        # get connection config from config-file
        self.config = configparser.ConfigParser()
        self.config.read(os.path.dirname(__file__) + '/../configs/postgres.cfg')
        defaults = self.config['DEFAULT']
        user = defaults['DB_USER']
        database = defaults['DB_NAME']
        password = defaults['DB_PASSWORD']
        host = defaults['DB_HOST']
        self.postgres_connection_string = f'postgresql://{user}:{password}@{host}:5432/{database}'
        self.connect()

    def connect(self) -> None:
        self.connection = psycopg2.connect(self.postgres_connection_string)
        self.cursor = self.connection.cursor()
        self.cursor.execute('set statement_timeout to 40000; commit;')

    def close(self) -> None:
        self.cursor.close()
        self.connection.close()

    def set_disabled_knobs(self, knobs: list) -> None:
        # todo enable all others rules before
        all_knobs = set(PostgresConnector.get_knobs())
        statements = ''
        for knob in all_knobs:
            if knob not in knobs:
                statements += f'SET {knob} to ON;'
        for knob in knobs:
            statements += f'SET {knob} to OFF;'
        self.cursor.execute(statements)

    def get_knob(self, knob: str) -> bool:
        """Get current status of a knob"""
        self.cursor.execute(f'SELECT current_setting(\'{knob}\')')
        result = self.cursor.fetchone()[0] == 'on'
        return result

    def explain(self, query: str) -> str:
        """Explain a query and return the json query plan"""
        self.cursor.execute(f'EXPLAIN (FORMAT JSON) {query}')
        return json.dumps(self.cursor.fetchone()[0][0]['Plan'])

    def execute(self, query: str) -> DBConnector.TimedResult:
        """Execute the query and return its result"""
        begin = time.time_ns()
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        elapsed_time_usec = int((time.time_ns() - begin) / 1_000)

        return DBConnector.TimedResult(result, elapsed_time_usec)

    @staticmethod
    def get_name() -> str:
        return 'postgres'

    @staticmethod
    def get_knobs() -> list:
        """Static method returning all knobs defined for this connector"""
        with open(os.path.dirname(__file__) + '/../knobs/postgres.txt', 'r', encoding='utf-8') as f:
            return [line.replace('\n', '') for line in f.readlines()]

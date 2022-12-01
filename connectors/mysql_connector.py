"""This module provides a connection to the MySql database for benchmarking"""
import os
from connectors.connector import DBConnector
import time
import mysql.connector
import configparser


class MySqlConnector(DBConnector):
    """This class handles the connection to the benchmarked MySQL database"""

    def __init__(self):
        # connection details
        super().__init__()
        # get connection config from config-file
        self.config = configparser.ConfigParser()
        self.config.read(os.path.dirname(__file__) + '/../configs/mysql.cfg')
        defaults = self.config['DEFAULT']

        self.user = defaults['USER']
        self.database = defaults['DATABASE']
        self.password = defaults['PASSWORD']
        self.host = defaults['HOST']
        self.connect()

    def connect(self):
        # connect to the MySql server
        self.connection = mysql.connector.connect(user=self.user, password='', host='127.0.0.1', database=self.database)
        self.cursor = self.connection.cursor(buffered=True)
        self.cursor.execute('SET GLOBAL interactive_timeout=40000')

    def close(self):
        self.cursor.close()
        self.connection.close()

    def set_disabled_knobs(self, knobs: list) -> None:
        """Toggle a list of knobs"""
        all_knobs = MySqlConnector.get_knobs()
        for knob in all_knobs:
            self.cursor.execute(f'SET SESSION optimizer_switch = \'{knob}=on\'')
        for switch in knobs:
            self.cursor.execute(f'SET SESSION optimizer_switch = \'{switch}=off\'')

    def explain_plan(self, query):
        self.cursor.execute(f'EXPLAIN FORMAT=JSON {query}')
        result = self.cursor.fetchall()
        self.cursor.execute('SELECT @@optimizer_switch')
        print(self.cursor.fetchall()[0][0])
        return result

    def get_knob(self, knob: str) -> bool:
        """Get current status of a knob"""
        self.cursor.execute('SHOW SESSION VARIABLES LIKE \'optimizer_switch\'')
        knobs = self.cursor.fetchone()[1].split(',')
        for cur_knob in knobs:
            if cur_knob.startswith(knob):
                config = cur_knob.split('=')[1]
                return config == 'on'
        raise Exception(f'Knob {knob} not found by MySQL-Connector')

    def explain(self, query: str) -> str:
        """Explain a query and return its plan"""
        self.cursor.execute(f'EXPLAIN {query}')
        result = self.cursor.fetchone()
        result = str(result)
        return result

    def execute(self, query: str) -> DBConnector.TimedResult:
        """Execute the query and return its result"""
        begin = time.time_ns()
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        elapsed_time_usec = int((time.time_ns() - begin) / 1_000)
        return DBConnector.TimedResult(result, elapsed_time_usec)

    @staticmethod
    def get_name() -> str:
        return "mysql"

    @staticmethod
    def get_knobs() -> list:
        """Static method returning all knobs defined for this connector"""
        with open(os.path.dirname(__file__) + '/../knobs/mysql.txt', 'r', encoding='utf-8') as f:
            return [line.replace('\n', '') for line in f.readlines()]

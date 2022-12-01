"""This module provides a connection to a Spark cluster that is used for benchmarking"""
import pyspark
from pyspark.sql import SparkSession
import time
import re
import os
from custom_logging import autosteer_logging
from connectors.connector import DBConnector
import configparser

APP_ID = 0
PLAN_ID = 0
EXCLUDED_RULES = 'spark.sql.optimizer.excludedRules'


class SparkConnector(DBConnector):
    """This class implements the AutoSteer-G connector for a Spark cluster accepting SQL statements"""

    def __init__(self):
        super().__init__()
        # get connection config from config-file
        self.config = configparser.ConfigParser()
        self.config.read(os.path.dirname(__file__) + '/../configs/spark.cfg')
        defaults = self.config['DEFAULT']
        autosteer_logging.info('SparkSQL connector conntects to %s', defaults['SPARK_MASTER_URL'])
        self.spark_master_url = defaults['SPARK_MASTER_URL']
        self.data_location = defaults['DATA_LOCATION']

        # Set Spark Application Name
        global APP_ID
        self.app_name = APP_ID
        APP_ID += 1

        self.conf = pyspark.SparkConf()
        self.conf.setMaster(self.spark_master_url)
        self.spark_session = None
        self._init_parquet_files()

    def connect(self):
        if self.spark_session is None:
            self.spark_session = SparkSession.builder.master(self.spark_master_url).appName(self.app_name).getOrCreate()
        SparkSession.getActiveSession()

    def close(self):
        if self.spark_session is not None:
            self.spark_session.stop()

    def _init_parquet_files(self):
        """For our experiments, data is stored in parquet files. We Create TempViews in PySpark for them"""
        if os.path.isdir(f'../{self.data_location}'):
            files = os.listdir(f'../{self.data_location}')
            for file in files:
                filename = f'{self.data_location}/{file}'
                print(f'Read parquet file: {filename}')
                parquet_table = self.spark_session.read.parquet(filename)
                parquet_table.createOrReplaceTempView(file.replace('.parquet', ''))
        else:
            autosteer_logging.fatal('SparkConnector cannot find the data directory containing the parquet files')

    def _postprocess_plan(self, plan):
        """Remove random ids from the explained query plan"""
        pattern = re.compile(r'#\d+L?|\(\d+\)|\[\d+\]')
        return re.sub(pattern, '', plan)

    def execute(self, query) -> DBConnector.TimedResult:
        begin = time.time_ns()
        collection = self.spark_session.sql(query).collect()
        elapsed_time_usecs = int((time.time_ns() - begin) / 1_000)
        autosteer_logging.info(f'QUERY RESULT: {str(collection)[:100] if len(str(collection)) > 100 else collection}')
        collection = 'EmptyResult' if len(collection) == 0 else collection[0]
        autosteer_logging.info(f'Hash(QueryResult) = {str(hash(str(collection)))}')

        return DBConnector.TimedResult(collection, elapsed_time_usecs)

    def explain(self, query):
        timed_result = self.execute(f'EXPLAIN FORMATTED {query}')
        return self._postprocess_plan(timed_result.result[0])

    def set_disabled_knobs(self, knobs) -> None:
        """Toggle a list of knobs"""
        if len(knobs) == 0:
            self.spark_session.conf.set(EXCLUDED_RULES, '')
        else:
            formatted_knobs = [f'org.apache.spark.sql.catalyst.optimizer.{rule}' for rule in knobs]
            self.spark_session.conf.set(EXCLUDED_RULES, ','.join(formatted_knobs))

    def get_knob(self, knob: str) -> bool:
        """Get current status of a knob"""
        exluded_rules = self.spark_session.conf.get(EXCLUDED_RULES)
        if exluded_rules is None:
            return True
        else:
            return not knob in exluded_rules

    @staticmethod
    def get_name() -> str:
        return "spark"

    @staticmethod
    def get_knobs() -> list:
        """Static method returning all knobs defined for this connector"""
        with open(os.path.dirname(__file__) + '/../knobs/spark.txt', 'r', encoding='utf-8') as f:
            return [line.replace('\n', '') for line in f.readlines()]

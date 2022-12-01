"""Run AutoSteer's training mode to explore alternative query plans"""
from typing import Type
import connectors.connector
import storage
from arguments_parser import get_parser
from autosteer.dp_exploration import run_query_with_optimizer_configs
from autosteer.query_span import run_get_query_span
from connectors import mysql_connector, duckdb_connector, postgres_connector, presto_connector, spark_connector
from custom_logging import autosteer_logging
import os
import sys


def approx_query_span_and_run(connector, path: str):
    run_get_query_span(connector, path)
    connector = connector()
    run_query_with_optimizer_configs(connector, path)


def get_connector_type(connector: str) -> Type[connectors.connector.DBConnector]:
    if connector == 'postgres':
        return postgres_connector.PostgresConnector
    elif connector == 'mysql':
        return mysql_connector.MySqlConnector
    elif connector == 'spark':
        return spark_connector.SparkConnector
    elif connector == 'presto':
        return presto_connector.PrestoConnector
    elif connector == 'duckdb':
        return duckdb_connector.DuckDBConnector
    autosteer_logging.fatal('Unknown connector %s', connector)


if __name__ == '__main__':
    args = get_parser().parse_args()

    ConnectorType = get_connector_type(args.database)
    storage.TESTED_DATABASE = ConnectorType.get_name()

    if args.benchmark is None or not os.path.isdir(args.benchmark):
        autosteer_logging.fatal('Cannot access the benchmark directory containing the sql files with path=%s', args.benchmark)
        sys.exit(1)

    queries = sorted(list(filter(lambda q: q.endswith('.sql'), os.listdir(args.benchmark))))
    autosteer_logging.info('Found the following SQL files: %s', queries)
    for query in queries:
        autosteer_logging.info('run Q%s...', query)
        approx_query_span_and_run(ConnectorType, f'{args.benchmark}/{query}')

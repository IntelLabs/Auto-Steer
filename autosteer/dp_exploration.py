"""This module coordinates the query span approximation and the generation of new optimizer configurations for a query"""
import operator
import connectors.connector
import storage
import hashlib
from optimizer_config import OptimizerConfig
from functools import reduce
from custom_logging import autosteer_logging
from config import read_config


def hash_sql_result(s):
    """Generate a hash fingerprint for the result retrieved from the connector to assert that results are (probably) identical.
    Its important to round floats here, e.g. using 2 decimal places."""
    if len(s) == 0:  # empty result
        return 42
    flattened_result = reduce(operator.concat, s)
    normalized_result = list(sorted(map(lambda item: str(round(item, 1)) if isinstance(item, float) else str(item), flattened_result)))

    md5 = hashlib.md5()
    for item in normalized_result:
        md5.update(str(item).encode())
    return int.from_bytes(md5.digest()[:4], 'big')


def load_query(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        lines = filter(lambda s: not s.startswith('--') and not s == '', lines)
        return ''.join(lines).replace('\\s', ' ').replace(';', '')


def register_query_config_and_measurement(query_path, disabled_rules, logical_plan, timed_result=None, initial_call=False):
    def hash_query_plan(s):
        """Generate a hash fingerprint for the result retrieved from the connector to assert that results are (probably) identical.
        Its important to round floats here, e.g. using 2 decimal places."""
        flattened_result = reduce(operator.concat, s)
        normalized_result = tuple(map(lambda item: round(item, 2) if isinstance(item, float) else item, flattened_result))
        md5 = hashlib.md5()
        for item in normalized_result:
            md5.update(str(item).encode())
        return md5.hexdigest()

    result_fingerprint = hash_sql_result(timed_result.result) if timed_result is not None else None
    if timed_result is not None and not storage.register_query_fingerprint(query_path, result_fingerprint):
        autosteer_logging.warning('Result fingerprint=%s does not match existing fingerprints!', result_fingerprint)

    plan_hash = int(hash_query_plan(str(logical_plan)), 16) & ((1 << 31) - 1)

    is_duplicate = storage.register_query_config(query_path, disabled_rules, logical_plan, plan_hash)
    if is_duplicate:
        autosteer_logging.info('Plan hash already known')
    if not initial_call:
        storage.register_measurement(
            query_path,
            disabled_rules,
            walltime=timed_result.time_usecs,
            input_data_size=0,
            nodes=1)
    return is_duplicate


def run_query_with_optimizer_configs(connector: connectors.connector.DBConnector, query_path):
    """Use dynamic programming to find good optimizer configs"""
    autosteer_logging.info('Run Dynamic Programming for Query %s', query_path)
    sql_query = load_query(query_path)

    config = OptimizerConfig(query_path)
    num_duplicates = 0
    while config.has_next():
        optimizer_configs = config.next()
        connector.set_disabled_knobs(optimizer_configs)
        query_plan = connector.explain(sql_query)

        # check if a new query plan is generated
        if register_query_config_and_measurement(query_path, config.get_disabled_opts_rules(), query_plan, timed_result=None, initial_call=True):
            num_duplicates += 1
            continue

        run_config(config, connector, query_path, sql_query, query_plan)

    autosteer_logging.info('Found %s duplicated query plans!', num_duplicates)


def run_config(config: OptimizerConfig, connector: connectors.connector.DBConnector, query_path: str, sql_query: str, query_plan: str):
    for _ in range(int(read_config()['anysteer']['repeats'])):
        try:
            timed_result = connector.execute(sql_query)
        # pylint: disable=broad-except
        except Exception as e:
            autosteer_logging.fatal('Optimizer %s cannot be disabled for %s - skip this config. The error: %s', config.get_disabled_opts_rules(), query_path, e)
            break

        if register_query_config_and_measurement(query_path, config.get_disabled_opts_rules(), query_plan, timed_result):
            autosteer_logging.info('config results in already known query plan!')
            break

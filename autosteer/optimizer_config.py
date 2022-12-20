"""This module provides the HintSetExploration that runs AutoSteers dynamic-programming based exploration"""
from autosteer.query_span import QuerySpan
import storage
import pandasql as pdsql
import statistics
from utils.custom_logging import logger

MAX_DP_DEPTH = 3  # Define early termination of the dynamic-programming exploration.


def tuple_to_list(t):
    return [t[0]] if len(t) == 1 else list(t)


class HintSetExploration:
    """An OptimizerConfiguration coordinates the exploration of the hint-sets search space.
      It uses a dynamic programming-based approach to find promising hint-sets."""

    def __init__(self, query_path):
        self.query_span = QuerySpan(query_path)
        self.query_path = query_path
        self.tunable_knobs = self.query_span.get_tunable_knobs()  # the effective query optimizer knobs
        self.current_dp_level = 0
        self.blacklisted_hint_sets = set()  # store configs that resulted in running times worse than the baseline
        self.hint_sets = self.get_next_hint_sets()
        self.iterator = -1
        logger.info('Run %s different configs', len(self.hint_sets))

    def dp_combine(self, promising_disabled_opts, previous_configs):
        result = set()
        # Based on the results in previous stages, DP builds new promising configurations (a.k.a. hint-sets)
        for optimizer in promising_disabled_opts:
            # Combine with other results
            for conf in previous_configs:
                if optimizer[0] not in conf:
                    new_config = frozenset(conf + optimizer)
                    execute_config = True
                    for bad_config in self.blacklisted_hint_sets:
                        if bad_config.issubset(new_config):
                            execute_config = False
                            break
                    if execute_config:
                        result.add(new_config)
        return sorted([sorted(list(x)) for x in result])

    def check_config_for_dependencies(self, config):
        """Check if there is an alternative optimizer in the config. If yes, check that all dependencies are disabled as well."""
        for optimizer in config:
            if optimizer in self.query_span.dependencies:
                if not frozenset(self.query_span.dependencies[optimizer]).issubset(config):
                    return False
        return True

    def __repr__(self):
        return f'Config {{\n\toptimizers:{self.tunable_knobs}}}'

    def get_measurements(self):
        """Get all measurements collected so far for the current query"""
        stmt = '''
            SELECT walltime as total_runtime, qoc.disabled_rules, m.time, qoc.num_disabled_rules
            FROM queries q,
                 measurements m,
                 query_optimizer_configs qoc
            WHERE m.query_optimizer_config_id = qoc.id
              AND qoc.query_id = q.id
              AND q.query_path = :query_path
            order by m.time asc;
            '''
        return storage.get_df(stmt, {'query_path': self.query_path})

    def get_baseline(self):
        """Get all measurements of the default plan"""
        df = self.get_measurements()  # pylint: disable=possibly-unused-variable
        runs = pdsql.sqldf('SELECT total_runtime FROM df WHERE disabled_rules = \'None\'', locals())
        runtimes = runs['total_runtime'].to_list()
        return runtimes

    def get_promising_measurements_by_num_rules(self, num_disabled_rules, baseline_median, baseline_mean):
        """Get all measurements for hint-sets having a specific size"""
        measurements = self.get_measurements()
        stmt = f'''SELECT total_runtime, disabled_rules, time
        FROM measurements
        WHERE num_disabled_rules = {num_disabled_rules};'''
        df = pdsql.sqldf(stmt, locals())
        measurements = df.groupby(['disabled_rules'])['total_runtime'].agg(['median', 'mean'])

        # Identify bad hint-sets and blacklist them for later DP stages
        bad_hint_sets = measurements[(measurements['median'] > baseline_median) | (measurements['mean'] > baseline_mean)]
        for config in bad_hint_sets.index.values.tolist():
            self.blacklisted_hint_sets.add(frozenset(config.split(',')))

        # Find hint-sets performing better than the default plan
        good_hint_sets = measurements[(measurements['median'] < baseline_median) & (measurements['mean'] <= baseline_mean)]
        configs = good_hint_sets.index.values.tolist()
        configs = filter(lambda n: n != 'None', configs)
        return [conf.split(',') for conf in configs]

    # Create the next hint-sets (a.k.a. configs) starting with one disabled optimizer and switch to dynamic programming later
    def get_next_hint_sets(self):
        n = self.current_dp_level
        if n > len(self.tunable_knobs) or n > MAX_DP_DEPTH:
            return None
        elif n == 0:
            configs = [[]]
        elif n == 1:
            configs = [[opt] for opt in self.tunable_knobs]
        else:
            baseline = self.get_baseline()
            try:
                # Basic statistics of the default plan (= baseline)
                median = statistics.median(baseline)
                mean = statistics.mean(baseline)
                # Fetch results from previous runs, consider only those hint-sets that were better than the baseline
                single_optimizers = self.get_promising_measurements_by_num_rules(1, median, mean) + [[key] for key in self.query_span.dependencies]
                combinations_previous_run = self.get_promising_measurements_by_num_rules(n - 1, median, mean)
                # Leverage hint-sets from n-1 and combine with n=1
                configs = self.dp_combine(single_optimizers, combinations_previous_run)
            except ArithmeticError as err:
                logger.warning('DP: get_next_hint_sets() results in an ArithmeticError %s', err)
                configs = None
        self.current_dp_level += 1
        # Remove these configs where a knob has unmet dependencies (e.g. its dependent optimizers are not part of the config)
        configs = list(filter(self.check_config_for_dependencies, configs))
        return configs

    def get_disabled_opts_rules(self):
        if self.hint_sets is None or len(self.hint_sets) == 0 or len(self.hint_sets[self.iterator]) == 0:
            return None
        return ','.join(sorted(tuple_to_list(self.hint_sets[self.iterator])))

    def has_next(self):
        if self.iterator < len(self.hint_sets) - 1:
            return True
        # Proceed to the next dynamic-programming level
        self.hint_sets = self.get_next_hint_sets()
        if self.hint_sets is None:
            return False
        logger.info('Enter next DP stage, execute for %s hint sets/configurations', len(self.hint_sets))
        self.iterator = -1
        return self.iterator < len(self.hint_sets) - 1

    def next(self):
        """Returns the next hint-set"""
        self.iterator += 1
        conf = self.hint_sets[self.iterator]
        return list(filter(lambda x: x in self.query_span.get_tunable_knobs(), conf))

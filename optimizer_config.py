"""This module provides the classes QuerySpan and the OptimizerConfiguration"""
from autosteer import query_span
import storage
import pandasql as pdsql
import statistics
import progressbar
from custom_logging import autosteer_logging
from hint_set import HintSet

# define early stopping and do not explore all possible optimizer configurations
MAX_DP_DEPTH = 3


def tuple_to_list(t):
    if len(t) == 1:
        return [t[0]]
    else:
        return list(t)


class QuerySpan:
    """This implementation is specific to presto, as it differentiates rules and optimizers"""

    def __init__(self, query_path=None):
        if query_path is not None:
            self.query_path = query_path
            self.effective_optimizers = storage.get_effective_optimizers(self.query_path)
            self.required_optimizers = storage.get_required_optimizers(self.query_path)
            self._get_dependencies()

    def _get_dependencies(self):
        """Alternative optimizers become only effective if their dependencies are also deactivated"""
        dependencies = storage.get_effective_optimizers_depedencies(self.query_path)
        self.dependencies = {}
        for optimizer, dependency in dependencies:
            if optimizer in self.dependencies:
                self.dependencies[optimizer].append(dependency)
            else:
                self.dependencies[optimizer] = [dependency]

    def get_tunable_optimizers(self):
        return sorted(list(set(self.effective_optimizers).difference(self.required_optimizers)))


class OptimizerConfig:
    """An OptimizerConfig allows to efficiently explore the search space of different optimizer settings.
      It implements a dynamic programming-based approach to execute promising optimizer configurations (e.g. disable certain optimizers)"""

    def __init__(self, query_path):
        self.query_path = query_path
        # store configs that resulted in runtimes worse than the baseline
        self.blacklisted_configs = set()
        self.query_span = QuerySpan(self.query_path)
        self.tunable_opts_rules = self.query_span.get_tunable_optimizers()

        self.n = 0  # consider 1 rule/optimizer at once
        self.configs = self.get_next_configs()
        self.iterator = -1

        self.progress_bar = None
        self.restart_progress_bar()
        print(f'\trun {self.get_num_configs()} different configs')

    def dp_combine(self, promising_disabled_opts, previous_configs):
        result = set()
        # based on previous results, use DP to build new interesting configurations
        for optimizer in promising_disabled_opts:
            # combine with all other results
            for conf in previous_configs:
                if optimizer[0] not in conf:
                    new_config = frozenset(conf + optimizer)
                    execute_config = True
                    for bad_config in self.blacklisted_configs:
                        if bad_config.issubset(new_config):
                            execute_config = False
                            break
                    if execute_config:
                        result.add(new_config)
        return sorted([sorted(list(x)) for x in result])  # key=lambda x: ''.join(x)

    def check_config_for_dependencies(self, config):
        """Check if there is an alternative optimizer in the config. If yes, check that all dependencies are disabled as well."""
        for optimizer in config:
            if optimizer in self.query_span.dependencies:
                if not frozenset(self.query_span.dependencies[optimizer]).issubset(config):
                    return False
        return True

    def restart_progress_bar(self):
        if self.progress_bar is not None:
            self.progress_bar.finish()
        self.progress_bar = progressbar.ProgressBar(
            maxval=self.get_num_configs(),
            widgets=[progressbar.Bar('=', '[', ']'), ' '])
        self.progress_bar.start()

    def __repr__(self):
        return f'Config {{\n\toptimizers:{self.tunable_opts_rules}}}'

    def get_measurements(self):
        # we do not consider planning and scheduling time in the total runtime
        stmt = f'''
            select walltime as total_runtime, qoc.disabled_rules, m.time, qoc.num_disabled_rules
            from queries q,
                 measurements m,
                 query_optimizer_configs qoc
            where m.query_optimizer_config_id = qoc.id
              and qoc.query_id = q.id
              and q.query_path = '{self.query_path}'
              and (qoc.num_disabled_rules = 1 or qoc.duplicated_plan = false)
            order by m.time asc;
            '''
        df = storage.get_df(stmt)
        return df

    def get_baseline(self):
        # pylint: disable=possibly-unused-variable
        df = self.get_measurements()
        runs = pdsql.sqldf('select total_runtime from df where disabled_rules = \'None\'', locals())
        runtimes = runs['total_runtime'].to_list()
        return runtimes

    def get_promising_measurements_by_num_rules(self, num_disabled_rules, baseline_median, baseline_mean):
        measurements = self.get_measurements()
        stmt = f'''select total_runtime, disabled_rules, time
        from measurements
        where num_disabled_rules = {num_disabled_rules};
        '''
        df = pdsql.sqldf(stmt, locals())
        measurements = df.groupby(['disabled_rules'])['total_runtime'].agg(['median', 'mean'])

        # find bad configs and black list them so they are not used in later DP stages
        bad_configs = measurements[(measurements['median'] > baseline_median) |
                                   (measurements['mean'] > baseline_mean)]
        for config in bad_configs.index.values.tolist():
            opts = config.split(',')
            self.blacklisted_configs.add(frozenset(opts))

        # find good configs which are better than the default config with all optimizers enabled
        good_configs = measurements[(measurements['median'] < baseline_median)
                                    & (measurements['mean'] <= baseline_mean)]

        configs = good_configs.index.values.tolist()
        configs = filter(lambda n: n != 'None', configs)
        return [conf.split(',') for conf in configs]

    # create the next configs starting with one disabled optimizer and then, switch to dynamic programming
    def get_next_configs(self):
        n = self.n
        if n > len(self.tunable_opts_rules) or n > MAX_DP_DEPTH:
            return None
        elif n == 0:
            configs = [[]]
        elif n == 1:
            configs = [[opt] for opt in self.tunable_opts_rules]
        else:
            # build config based on DP
            baseline = self.get_baseline()
            try:
                # basic statistics for baseline
                median = statistics.median(baseline)
                mean = statistics.mean(baseline)
                # get results from previous runs, consider only those configs better than the baseline
                single_optimizers = self.get_promising_measurements_by_num_rules(1, median, mean) + [[key] for key in self.query_span.dependencies]
                combinations_previous_run = self.get_promising_measurements_by_num_rules(n - 1, median, mean)
                # use configs from n-1 and combine with n=1
                configs = self.dp_combine(single_optimizers, combinations_previous_run)
            except ArithmeticError as err:
                autosteer_logging.info('DP: get_next_configs() results in an ArithmeticError %s', err)
                configs = None
        self.n += 1
        # remove those configs where an optimizer exists that has unfulfilled dependencies (e.g. its dependencies are not part of the config)
        configs = list(filter(self.check_config_for_dependencies, configs))
        return configs

    def get_num_configs(self):
        return len(self.configs)

    def get_disabled_opts_rules(self):
        if self.configs is None or len(self.configs) == 0 or len(self.configs[self.iterator]) == 0:
            return None
        return ','.join(sorted(tuple_to_list(self.configs[self.iterator])))

    def has_next(self):
        if self.iterator < self.get_num_configs() - 1:
            return True
        self.configs = self.get_next_configs()
        if self.configs is None:
            return False
        autosteer_logging.info('Enter next DP stage, execute for %s hint sets/configurations', len(self.configs))
        self.restart_progress_bar()
        self.iterator = -1
        return self.iterator < self.get_num_configs() - 1

    def next(self):
        self.iterator += 1
        self.progress_bar.update(self.iterator)
        conf = self.configs[self.iterator]
        tmp_optimizers = list(filter(lambda x: x in self.query_span.get_tunable_optimizers(), conf))

        commands = tmp_optimizers
        # if len(tmp_optimizers) > 0:
        #     commands.append(f'''SET session {BAO_DISABLED_OPTIMIZERS} = \'{','.join(tmp_optimizers)}\'''')
        return commands


class OptimizerConfigForPredictions:
    """An OptimizerConfig allows to efficiently explore the search space of different optimizer settings using Bao's predictions.
      It implements a dynamic programming-based approach to execute promising optimizer configurations (e.g. disable certain optimizers)"""

    def __init__(self, query_path, baseline: query_span.HintSet, configs: list[query_span.HintSet]):
        self.baseline = baseline
        self.tunable_opts_rules = list(filter(lambda config: config.predicted_runtime < baseline.predicted_runtime, configs))
        self.query_path = query_path
        # store configs that resulted in runtimes worse than the baseline
        self.blacklisted_configs = set()
        self.results_last_iteration = [HintSet([opt]) for opt in configs]
        self.n = 2  # start with dp iteration 2
        # keep track of that hint set with the lowest predicted runtime
        self.min_hint_set = self.results_last_iteration[0]
        for hs in self.results_last_iteration[1:]:
            if hs.predicted_runtime < self.min_hint_set.predicted_runtime:
                self.min_hint_set = hs

    def dp_combine(self, promising_disabled_opts, previous_configs: list[HintSet]) -> set[HintSet]:
        result = set()
        # based on previous results, use DP to build new interesting configurations
        for optimizer in promising_disabled_opts:
            # combine with all other results
            for conf in previous_configs:
                if optimizer not in conf.optimizers:
                    new_config = HintSet(list(conf.optimizers) + [optimizer])
                    execute_config = True
                    for bad_config in self.blacklisted_configs:
                        if bad_config.issubset(new_config):
                            execute_config = False
                            break
                    if execute_config:
                        result.add(new_config)
        return set(filter(lambda r: r.is_valid(), result))  # key=lambda x: ''.join(x)

    def __repr__(self):
        return f'Config {{\n\toptimizers:{self.tunable_opts_rules}}}'

    # create the next configs starting with one disabled optimizer and then, switch to dynamic programming
    def get_next_configs(self):
        n = self.n
        if n > len(self.tunable_opts_rules) or n > MAX_DP_DEPTH:
            return None
        else:
            # build config based on DP
            # basic statistics for baseline
            median = self.baseline.predicted_runtime
            mean = self.baseline.predicted_runtime
            try:
                # get results from previous runs, consider only those configs better than the baseline
                single_optimizers = self.tunable_opts_rules  # todo add alternative optimizers here
                combinations_previous_run = self.results_last_iteration  # todo filter for good combos
                # use configs from n-1 and combine with n=1
                configs = self.dp_combine(single_optimizers, combinations_previous_run)
            except ArithmeticError as err:
                autosteer_logging.info('DP: get_next_configs() results in an ArithmeticError %s', err)
                configs = None
        # remove those configs where an optimizer exists that has unfulfilled dependencies (e.g. its dependencies are not part of the config)
        return configs

    def get_disabled_opts_rules(self):
        pass

    def has_next(self):
        self.configs = self.get_next_configs()
        if self.configs is not None and len(self.configs) > 0:
            return True
        return False

    def next(self, last_configs: list[HintSet]):
        self.n += 1
        self.results_last_iteration = last_configs
        for hs in self.results_last_iteration:
            if hs.predicted_runtime < self.min_hint_set.predicted_runtime:
                print('updated best predicted hint set')
                self.min_hint_set = hs

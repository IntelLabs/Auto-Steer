"""This module implements a generic but naive approach to approximate the query span. A system integration will be much more efficient."""
import queue
from multiprocessing.pool import ThreadPool as Pool
import numpy as np
import storage
from utils.custom_logging import logger
from utils.config import read_config
from utils.util import flatten

N_THREADS = int(read_config()['autosteer']['explain_threads'])
FAILED = 'FAILED'


class HintSet:
    """A hint-set describing the disabled knobs; may have dependencies to other hint-sets"""

    def __init__(self, knobs, dependencies):
        self.knobs: set = knobs
        self.dependencies: HintSet = dependencies
        self.plan = None  # store the json query plan
        self.required = False
        self.predicted_runtime = -1.0

    def get_all_knobs(self) -> list:
        """Return all (including the dependent) knobs"""
        return list(self.knobs) + (self.dependencies.get_all_knobs() if self.dependencies is not None else [])

    def __str__(self):
        res = '' if self.dependencies is None else (',' + str(self.dependencies))
        return ','.join(self.knobs) + res


def get_query_plan(args: tuple) -> HintSet:
    connector_type, sql_query, hintset = args
    connector = connector_type()
    knobs = hintset.get_all_knobs()
    connector.set_disabled_knobs(knobs)
    hintset.plan = connector.explain(sql_query)
    return hintset


def approximate_query_span(connector_type, sql_query: str, get_json_query_plan, find_alternative_knobs=False, batch_wise=False) -> list[HintSet]:
    # Create singleton hint-sets
    knobs = np.array(connector_type.get_knobs())
    hint_sets = np.array([HintSet({knob}, None) for knob in knobs])
    # To speed up the query span approximation, we can submit multiple queries in parallel
    with Pool(N_THREADS) as thread_pool:
        query_span: list[HintSet] = []
        default_plan = get_json_query_plan((connector_type, sql_query, HintSet(set(), None)))
        query_span.append(default_plan)

        args = [(connector_type, sql_query, knob) for knob in hint_sets]
        results = np.array(list(thread_pool.map(get_json_query_plan, args)))

        default_plan_hash = hash(str(default_plan.plan))
        logger.info('Default plan hash: #%s', default_plan_hash)
        failed_plan_hash = hash(FAILED)
        logger.info('Failed query hash: #%s', failed_plan_hash)

        hashes = np.array(list(thread_pool.map(lambda res: hash(str(res.plan)), results)))
        effective_knobs_indexes = np.where((hashes != default_plan_hash) & (hashes != failed_plan_hash))
        required_knobs_indexes = np.where(hashes == failed_plan_hash)
        logger.info('There are %s alternative plans', effective_knobs_indexes[0].size)

        new_effective_knobs = queue.Queue()
        for optimizer in results[effective_knobs_indexes]:
            new_effective_knobs.put(optimizer)

        required_optimizers = results[required_knobs_indexes]
        for required_optimizer in required_optimizers:
            required_optimizer.required = True
            query_span.append(required_optimizer)

        # Note that indices change after delete
        hint_sets = np.delete(hint_sets, np.concatenate([effective_knobs_indexes, required_knobs_indexes], axis=1))

        # Detect rules that become effective only if other rules are disabled
        if find_alternative_knobs:
            if batch_wise:  # Batch approximation: according to Negi et al. 2021 "Steering query optimizers: A practical take on big data workloads"
                found_new_knobs = True
                effective_knobs = [results[index] for index in effective_knobs_indexes[0]]
                disabled_knobs = flatten([hs.knobs for hs in effective_knobs])
                all_effective_knobs = HintSet(set(disabled_knobs), None)

                while found_new_knobs:
                    for optimizer in results[effective_knobs_indexes]:
                        query_span.append(optimizer)
                    default_plan = get_json_query_plan((connector_type, sql_query, all_effective_knobs))
                    default_plan_hash = hash(default_plan.plan)
                    args = [(connector_type, sql_query, HintSet(set(hs.knobs), all_effective_knobs)) for hs in hint_sets]
                    results = np.array(list(thread_pool.map(get_json_query_plan, args)))
                    hashes = np.array(list(thread_pool.map(lambda res: hash(res.plan), results)))
                    effective_knobs_indexes = np.where((hashes != default_plan_hash) & (hashes != failed_plan_hash))
                    new_alternative_knobs = results[effective_knobs_indexes]
                    for new_alternative_knob in new_alternative_knobs:
                        all_effective_knobs = HintSet(new_alternative_knob.knobs, all_effective_knobs)

                    found_new_knobs = len(effective_knobs_indexes[0]) > 0
            else:  # Iterative approximation: disable one knob at a time and check if other knobs become effective
                while not new_effective_knobs.empty():
                    effective_knob = new_effective_knobs.get()
                    query_span.append(effective_knob)
                    default_plan_hash = hash(effective_knob.plan)
                    args = [(connector_type, sql_query, HintSet(hs.knobs, effective_knob)) for hs in hint_sets]
                    results = np.array(list(thread_pool.map(get_json_query_plan, args)))
                    hashes = np.array(list(thread_pool.map(lambda res: hash(res.plan), results)))
                    effective_knobs_indexes = np.where((hashes != default_plan_hash) & (hashes != failed_plan_hash))
                    new_alternative_knobs = results[effective_knobs_indexes]

                    # Add new alternative optimizers to the queue, remove them from the knobs
                    for alternative_optimizer in new_alternative_knobs:
                        new_effective_knobs.put(alternative_optimizer)
                        for i in reversed(range(len(hint_sets))):
                            if hint_sets[i] == alternative_optimizer:
                                hint_sets = np.delete(hint_sets, i)
                                break
        else:
            while not new_effective_knobs.empty():
                new_effective_optimizer = new_effective_knobs.get()
                query_span.append(new_effective_optimizer)
    return query_span


def serialize_dependencies(query_path: str, hint_set: HintSet):
    if hint_set.dependencies is not None:
        for knob in hint_set.dependencies.get_all_knobs():
            storage.register_optimizer_dependency(query_path, ','.join(sorted(hint_set.knobs)), knob)


def run_get_query_span(connector_type, benchmark, query):
    query_path = f'{benchmark}/{query}'
    logger.info('Approximate query span for query: %s', query_path)
    storage.register_query(query_path)

    sql = storage.read_sql_file(query_path)
    query_span = approximate_query_span(connector_type, sql, get_query_plan, find_alternative_knobs=True, batch_wise=False)

    # Serialize the approximated query span in the database
    for optimizer in query_span:  # pylint: disable=not-an-iterable
        logger.info('Found new hint-set: %s', optimizer)
        storage.register_optimizer(query_path, ','.join(sorted(optimizer.knobs)), optimizer.required)
        serialize_dependencies(query_path, optimizer)


class QuerySpan:
    """A wrapper class for query spans which are reconstructed from storage."""

    def __init__(self, query_path=None):
        if query_path is not None:
            self.query_path = query_path
            self.effective_optimizers = storage.get_effective_optimizers(self.query_path)
            self.required_optimizers = storage.get_required_optimizers(self.query_path)
            self.dependencies = {}
            self._get_dependencies()

    def _get_dependencies(self):
        """Alternative optimizers become only effective if their dependencies are also disabled"""
        dependencies = storage.get_effective_optimizers_depedencies(self.query_path)
        for optimizer, dependency in dependencies:
            if optimizer in self.dependencies:
                self.dependencies[optimizer].append(dependency)
            else:
                self.dependencies[optimizer] = [dependency]

    def get_tunable_knobs(self):
        return sorted(list(set(self.effective_optimizers).difference(self.required_optimizers)))

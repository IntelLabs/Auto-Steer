"""Preprocess Presto json query plans before passing them to Bao"""
import numpy as np
from utils.custom_logging import logger
from inference.preprocessing.preprocessor import QueryPlanPreprocessor

# The following operators can be found in Presto query plans
OUTPUT = 'Output'
AGGREGATE = 'AGGREGATE'
AGGREGATE_FINAL = 'Aggregate(FINAL)'
AGGREGATE_PARTIAL = 'Aggregate(PARTIAL)'
LOCAL_EXCHANGE = 'LocalExchange'
REMOTE_EXCHANGE = 'RemoteStreamingExchange'
FILTER = 'Filter'
TABLE_SCAN = 'TableScan'
PROJECT = 'Project'
INNER_JOIN = 'InnerJoin'
CROSS_JOIN = 'CrossJoin'
SEMI_JOIN = 'SemiJoin'
LEFT_JOIN = 'LeftJoin'
RIGHT_JOIN = 'RightJoin'
VALUES = 'Values'

# Composite operators
SCAN_FILTER_PROJECT = 'ScanFilterProject'
SCAN_FILTER = 'ScanFilter'
SCAN_PROJECT = 'ScanProject'

UNARY_OPERATORS = [OUTPUT, AGGREGATE, AGGREGATE_PARTIAL, AGGREGATE_FINAL, LOCAL_EXCHANGE, REMOTE_EXCHANGE, SCAN_FILTER_PROJECT, FILTER, PROJECT, TABLE_SCAN,
                   VALUES]
BINARY_OPERATORS = [INNER_JOIN, CROSS_JOIN, SEMI_JOIN, LEFT_JOIN, RIGHT_JOIN]
LEAF_TYPES = [TABLE_SCAN, VALUES]
ENCODED_TYPES = list(sorted(list(set(UNARY_OPERATORS + BINARY_OPERATORS) - {OUTPUT, SCAN_FILTER_PROJECT, SCAN_FILTER, SCAN_PROJECT})))

# The following attributes can be found in each node of a presto query plan
CHILDREN = 'children'
NODE_TYPE = 'name'
ESTIMATES = 'estimates'
TABLE_NAME = 'tableName'
PREPROCESSED = 'preprocessed'

# The following attributes can be found in the presto query plan statistics of leaf nodes (e.g. TableScan, ScanFilterProject, etc.)
ROWS = 'rows'
ROW_SIZE = 'rowsSize'
CPU_COST = 'cpuCost'
MAX_MEMORY = 'maxMemory'
MAX_MEMORY_OUTPUT = 'maxMemoryWhenOutputting'
NETWORK_COST = 'networkCost'


class MalformedQueryPlanException(Exception):
    pass


class TreeBuilderException(Exception):
    def __init__(self, msg):
        Exception.__init__(self, msg)


def is_binary_operator(node):
    return node[NODE_TYPE] in BINARY_OPERATORS


def is_unary_operator(node):
    return node[NODE_TYPE] in UNARY_OPERATORS


def is_leaf_operator(node):
    return node[NODE_TYPE] in LEAF_TYPES


class TreeBuilder:
    """This class gets invoked by the TreeFeaturizer; it preprocesses a query plan before it is fed to the TCNN"""

    def __init__(self, stats_extractor, relations):
        self.__stats = stats_extractor
        # pylint: disable=unused-private-member
        self.__relations = sorted(relations)

    def __featurize_binary_operator(self, node):
        assert is_binary_operator(node)
        arr = np.zeros(len(ENCODED_TYPES) + 1)
        arr[ENCODED_TYPES.index(node[NODE_TYPE])] = 1
        return np.concatenate((arr, self.__stats(node)))

    def __featurize_unary_operator(self, node):
        assert is_unary_operator(node)
        arr = np.zeros(len(ENCODED_TYPES) + 1)
        arr[ENCODED_TYPES.index(node[NODE_TYPE])] = 1
        return np.concatenate((arr, self.__stats(node)))

    def __featurize_null_operator(self):
        arr = np.zeros(len(ENCODED_TYPES) + 1)
        arr[-1] = 1  # declare as null vector
        return np.concatenate((arr, self.__stats.get_null_stats()))

    def plan_to_feature_tree(self, node):
        """This method recursively traverses the query plan and returns a feature tree"""
        children = node[CHILDREN] if CHILDREN in node else []
        # do not encode output nodes and combined table_scans
        if node[NODE_TYPE] not in ENCODED_TYPES:
            assert len(children) == 1
            return self.plan_to_feature_tree(children[0])

        if is_binary_operator(node):
            assert len(children) == 2
            featurized_node = self.__featurize_binary_operator(node)
            left = self.plan_to_feature_tree(children[0])
            right = self.plan_to_feature_tree(children[1])
            return featurized_node, left, right

        if is_leaf_operator(node):
            assert not children
            return self.__featurize_unary_operator(node)

        if is_unary_operator(node):
            child = self.plan_to_feature_tree(children[0])
            assert len(children) <= 1
            return self.__featurize_unary_operator(node), child, self.__featurize_null_operator()

        raise TreeBuilderException('Node was neither transparent, nor a join or a scan: ' + str(node))


def _normalize(x, lo, hi):
    if hi == lo:
        logger.warning('[WARNING] normalization divide by zero')
        return np.infty if (np.log(x + 1) - lo) > 0 else -np.infty
    return (np.log(x + 1) - lo) / (hi - lo)


def _get_buffer_count_for_leaf(leaf, buffers):
    total = 0
    if TABLE_NAME in leaf:
        total += buffers.get(leaf[TABLE_NAME], 0)

    if 'Index Name' in leaf:
        total += buffers.get(leaf['Index Name'], 0)

    return total


class StatExtractor:
    """Extract statistics such as min and max from a query plan"""

    def __init__(self, fields, mins, maxs):
        self.__fields = fields
        self.__mins = mins
        self.__maxs = maxs

    def __call__(self, inp):
        estimates = inp[ESTIMATES] if ESTIMATES in inp else {}
        res = []
        for f, lo, hi in zip(self.__fields, self.__mins, self.__maxs):
            if f not in estimates or estimates[f] == 0.0 or estimates[
                f] == 'NaN':
                res += [0, 0]
            else:
                res += [1, _normalize(np.log(estimates[f] + 1), lo, hi)]
        return res

    def get_null_stats(self):
        # create null value
        return [0.0] * (2 * len(self.__fields))


def _get_plan_stats(data):
    costs = []
    rows = []

    def process_estimates(node):
        estimates = node[ESTIMATES]
        cpu_estimate = estimates[CPU_COST]
        rows_estimate = estimates[ROWS]
        if not cpu_estimate in (0, 'NaN'):
            costs.append(cpu_estimate)
        if not rows_estimate in (0, 'NaN'):
            rows.append(rows_estimate)

    def recurse(node):
        if ESTIMATES in node:
            process_estimates(node)

        if CHILDREN in node:
            for child in node[CHILDREN]:
                recurse(child)

    for plan in data:
        recurse(plan)

    costs = np.array(costs)
    rows = np.array(rows)

    costs = np.log(costs + 1)
    rows = np.log(rows + 1)

    costs_min = 0 if len(costs) == 0 else np.min(costs)
    costs_max = 1 if len(costs) == 0 else np.max(costs)
    rows_min = 0 if len(rows) == 0 else np.min(rows)
    rows_max = 1 if len(rows) == 0 else np.max(rows)

    return StatExtractor([CPU_COST, ROWS], [costs_min, rows_min], [costs_max, rows_max])


def _get_all_relations(data):
    all_rels = []

    def recurse(plan):
        if TABLE_NAME in plan:
            yield plan[TABLE_NAME]

        if CHILDREN in plan:
            for child in plan[CHILDREN]:
                yield from recurse(child)

    for plan in data:
        all_rels.extend(list(recurse(plan)))

    return set(all_rels)


def _attach_buf_data(tree):
    if 'Buffers' not in tree:
        return

    buffers = tree['Buffers']

    def recurse(n):
        if 'Plans' in n:
            for child in n['Plans']:
                recurse(child)
            return

        # it is a leaf
        n['Buffers'] = _get_buffer_count_for_leaf(n, buffers)

    recurse(tree['Plan'])


def _preprocess_scan_filter_project(plan):
    # assert len(plan['estimates']) == 3
    scan_node = {
        NODE_TYPE: TABLE_SCAN,
    }
    filter_node = {
        NODE_TYPE: FILTER,
        CHILDREN: [scan_node],
    }
    plan[NODE_TYPE] = PROJECT
    plan[CHILDREN] = [filter_node]
    plan[PREPROCESSED] = True


def _preprocess_scan_project(plan):
    scan_node = {NODE_TYPE: TABLE_SCAN}
    plan[NODE_TYPE] = PROJECT
    plan[CHILDREN] = [scan_node]
    plan[PREPROCESSED] = True


def _preprocess_scan_filter(plan):
    scan_node = {NODE_TYPE: TABLE_SCAN}
    plan[NODE_TYPE] = FILTER
    plan[CHILDREN] = [scan_node]
    plan[PREPROCESSED] = True


class PrestoPlanPreprocessor(QueryPlanPreprocessor):
    """"Transforms PrestoDB query plans into a form processable by the TCNNs"""

    def __init__(self):
        super().__init__()
        self.__tree_builder = None

    def fit(self, trees):
        for t in trees:
            self.preprocess(t)
            _attach_buf_data(t)
        all_rels = _get_all_relations(trees)
        stats_extractor = _get_plan_stats(trees)
        self.__tree_builder = TreeBuilder(stats_extractor, all_rels)

    def transform(self, trees):
        for tree in trees:
            self.preprocess(tree)
            _attach_buf_data(tree)
        return [self.__tree_builder.plan_to_feature_tree(tree) for tree in trees]

    def preprocess(self, plan):
        # Check if this plan has been preprocessed already
        if PREPROCESSED in plan and plan[PREPROCESSED]:
            return

        if plan[NODE_TYPE] == SCAN_FILTER_PROJECT:
            _preprocess_scan_filter_project(plan)
            return
        elif plan[NODE_TYPE] == SCAN_PROJECT:
            _preprocess_scan_project(plan)
            return
        elif plan[NODE_TYPE] == SCAN_FILTER:
            _preprocess_scan_filter(plan)
            return
        if ESTIMATES in plan:
            if len(plan[ESTIMATES]) == 0:
                plan.pop(ESTIMATES, None)
            elif len(plan[ESTIMATES]) == 1:
                plan[ESTIMATES] = plan[ESTIMATES][0]
            else:
                raise MalformedQueryPlanException('Multiple estimates for node!')

        plan[PREPROCESSED] = True
        if CHILDREN in plan:
            for child in plan[CHILDREN]:
                self.preprocess(child)

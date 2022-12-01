"""Parser for the arguments passed to the benchmark driver"""
import argparse


def get_parser():
    parser = argparse.ArgumentParser(description='CLI for PrestoDB with BAO-Query-Optimizer')
    parser.add_argument('--predict', help='used TCNN to predict which hint sets are best', action='store_true')
    parser.add_argument('--database', help='Which database connector should be used', type=str)
    parser.add_argument('--benchmark', help='path to a directory with SQL files', type=str)
    parser.add_argument('--explain', help='explain the query', action='store_true')
    parser.add_argument('--repeats', help='repeat queries', type=int, default=1)
    return parser

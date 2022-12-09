"""Parser for the arguments passed to the benchmark driver"""
import argparse


def get_parser():
    parser = argparse.ArgumentParser(description='CLI for AutoSteer')
    parser.add_argument('--training', help='use AutoSteer to generate training data', action='store_true')
    parser.add_argument('--inference', help='Leverage a TCNN in inference mode to predict which hint sets are best', action='store_true')
    parser.add_argument('--database', help='Which database connector should be used', type=str)
    parser.add_argument('--benchmark', help='path to a directory with SQL files', type=str)
    parser.add_argument('--explain', help='explain the query', action='store_true')
    parser.add_argument('--repeats', help='repeat benchmark', type=int, default=1)
    return parser

# Copyright 2022 Intel Corporation
# SPDX-License-Identifier: MIT
#
"""Parser for the arguments passed to the benchmark driver"""
import argparse


def get_parser():
    parser = argparse.ArgumentParser(description='CLI for AutoSteer')
    parser.add_argument('--training', help='use AutoSteer to generate training data', action='store_true', default=False)
    parser.add_argument('--inference', help='Leverage a TCNN in inference mode to predict which hint sets are best', action='store_true', default=False)
    parser.add_argument('--retrain', help='Retrain the TCNN', action='store_true', default=False)
    parser.add_argument('--create_datasets', help='Create the dataset before training the TCNN', action='store_true', default=False)
    parser.add_argument('--database', help='Which database connector should be used', type=str)
    parser.add_argument('--benchmark', help='path to a directory with SQL files', type=str)
    parser.add_argument('--explain', help='explain the query', action='store_true')
    parser.add_argument('--repeats', help='repeat benchmark', type=int, default=1)
    return parser

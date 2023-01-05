# Copyright 2022 Intel Corporation
# SPDX-License-Identifier: MIT
#
"""Define a custom logger for AutoSteer"""
import sys
import logging


def setup_custom_logger(name):
    formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    handler = logging.FileHandler('../benchmark.log', mode='w')
    handler.setFormatter(formatter)
    screen_handler = logging.StreamHandler(stream=sys.stdout)
    screen_handler.setFormatter(formatter)
    custom_logger = logging.getLogger(name)
    custom_logger.setLevel(logging.INFO)
    custom_logger.addHandler(handler)
    custom_logger.addHandler(screen_handler)
    return custom_logger


logger = setup_custom_logger('AUTOSTEER')

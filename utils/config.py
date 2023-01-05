# Copyright 2022 Intel Corporation
# SPDX-License-Identifier: MIT
#
"""Allows to easily access AutoSteer's config"""
import configparser


def read_config():
    config = configparser.ConfigParser()
    config.read('./config.cfg', encoding='utf-8')
    return config

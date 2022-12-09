"""Allows to easily access AutoSteer's config"""
import configparser


def read_config():
    config = configparser.ConfigParser()
    config.read('utils/config.cfg', encoding='utf-8')
    return config

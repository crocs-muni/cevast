"""This package provides tools for parsing certificate datasets."""

__all__ = ('RapidParser', 'get_parser_class', 'init_parser_from_cfg')
__version__ = '0.1'
__author__ = 'Radim Podola'

from cevast.dataset import DatasetType
from .rapid import RapidParser


def get_parser_class(dataset_type: DatasetType):
    """Provide parser class object based on the required dataset type"""
    if dataset_type == DatasetType.RAPID:
        return RapidParser

    return None


def init_parser_from_cfg(config):
    """Instantiate a parser class from the configuration file"""
    if config["type"] == DatasetType.RAPID:
        return RapidParser.from_config(config)

    return None

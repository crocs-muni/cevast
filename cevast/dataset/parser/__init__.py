"""This package provides tools for parsing certificate datasets."""

__all__ = ('RapidParser', 'getParser', 'getParserFromConfig')
__version__ = '0.1'
__author__ = 'Radim Podola'

from cevast.dataset import DatasetType
from .rapid7 import RapidParser


def getParserClass(dataset_type):
    if(dataset_type == DatasetType.RAPID):
        return RapidParser
    else:
        return None


def getParserFromConfig(config):
    if(config["type"] == DatasetType.RAPID):
        return RapidParser.fromConfig(config)
    else:
        return None

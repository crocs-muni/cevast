"""This package provides tools for collecting certificate datasets."""

__all__ = ('RapidCollector', 'CensysCollector', 'getCollector', 'getCollectorFromConfig')
__version__ = '0.1'
__author__ = 'Radim Podola'

from cevast.dataset import DatasetType

from .rapid7 import RapidCollector
from .censys import CensysCollector


def getCollector(dataset_type):
    if(dataset_type == DatasetType.RAPID):
        return RapidCollector
    elif(dataset_type == DatasetType.CENSYS):
        return CensysCollector
    else:
        return None


def getCollectorFromConfig(config):
    if(config["type"] == DatasetType.RAPID):
        return RapidCollector.fromConfig(config)
    elif(config["type"] == DatasetType.CENSYS):
        return CensysCollector.fromConfig(config)
    else:
        return None

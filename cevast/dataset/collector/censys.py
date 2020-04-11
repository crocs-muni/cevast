"""This module contains collector implementation for Censys dataset type"""

from cevast.dataset.dataset_manager import DatasetType

__author__ = 'Radim Podola'


class CensysCollector:

    _type = DatasetType.CENSYS

    def __init__(self, target_folder, url):
        print("I am CensysCollector: {}:{}".format(target_folder, url))

    # a class method to create a RapidCollector object from config file.
    @classmethod
    def fromConfig(cls, config):
        return cls(".", "//")

    def collect(self, date):
        raise NotImplementedError

"""This module contains collector implementation for Rapid dataset type"""
# https://opendata.rapid7.com/apihelp/

from cevast.dataset.dataset_manager import DatasetType

__author__ = 'Radim Podola'

class RapidCollector:

    _type = DatasetType.RAPID

    def __init__(self, target_folder, url):
        print("I am RapidCollector: {}:{}".format(target_folder, url))

    # a class method to create a RapidCollector object from config file.
    @classmethod
    def fromConfig(cls, config):
        return cls(".", "//")

    def collect(self, date):
        raise NotImplementedError

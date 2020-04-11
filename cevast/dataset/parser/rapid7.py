"""This module contains parser implementation for Rapid dataset type"""

from cevast.dataset.dataset_manager import DatasetType

__author__ = 'Radim Podola'


class RapidParser:

    _type = DatasetType.RAPID

    def __init__(self, target_folder, url):
        print("I am RapidParser: {}:{}".format(target_folder, url))

    # a class method to create a RapidParser object from config file.
    @classmethod
    def fromConfig(cls, config):
        return cls(".", "//")

    def parse(self, date):
        raise NotImplementedError

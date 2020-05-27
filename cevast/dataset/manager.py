"""
This module contains manager class interface and manager factory returning specific manager based on dataset type

Manager for managed certificate datasets
    - collected
    - parsed

    - will call correct parse()
"""

#import cevast.dataset.parser as dp
from enum import Enum


class DatasetType(Enum):
    RAPID = 1
    CENSYS = 2

"""
# TODO make ABC parser - NO! instead dataset_manager will have generic interface

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%d-%m-%y %H:%M:%S')

class DatasetManager():

    def __init__(self, dataset_type: DatasetType):
        if dataset_type in DatasetType:
            self._type = dataset_type
        else:
            raise NotImplementedError

    @classmethod
    def fromConfig(cls, config):
        if config['type'] not in DatasetType:
            raise NotImplementedError

        ins = cls(config['type'])
        ins._config = config
        return ins

    def __getParser(self):
        if self._parser is None:
            if self._config:
                return dp.getCollectorFromConfig(self._config)
            else:
                return dp.getParser

    def getDataset(self):
        pass

    @staticmethod
    def collect_dataset(date) -> Dataset:
        return None

    "Parsing output of every type will be generic file format: host, cert_chain""
    def parse_dataset(self, date) -> bool:
        self.getParser().parse(dataset)


class RapidDatasetManager(DatasetManager):
    C_CERT_NAME_SUFFIX = '-certs.gz'
    C_HOSTS_NAME_SUFFIX = '-hosts.gz'

    def __init__(self, workspace):
        self.__workspace = workspace
        pass

"""
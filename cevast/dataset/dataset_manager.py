import cevast.dataset.parser as dp
from enum import Enum


class DatasetType(Enum):
    RAPID = 1
    CENSYS = 2


class DatasetManager():

    def __init__(self, dataset_type):
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

    def getParser(self):
        if self._parser is None:
            if self._config:
                return dp.getCollectorFromConfig(self._config)
            else:
                return dp.getParser

"""This module contains DatasetManager factory implementation."""

from typing import Union
from cevast.dataset.dataset import DatasetType, DatasetInvalidError
from cevast.dataset.manager import DatasetManager
from cevast.dataset.rapid_manager import RapidDatasetManager


class DatasetManagerFactory:
    """Class implementing factory design patter for creation of DatasetManager objects."""

    __classes = {}

    # TODO automaticly search the package?
    @classmethod
    def load_classes(cls):
        """Utility function that initialize lookup dictionary with corresponding DatasetManager classes."""
        if cls.__classes:
            return
        cls.__classes[DatasetType.RAPID.name] = RapidDatasetManager
        # cls.__classes[DatasetType.CENSYS.name] = CensysManager

    @classmethod
    def create_manager(cls, dataset_type: Union[DatasetType, str]) -> DatasetManager:
        """Instantiate a corresponding DatasetManager class based on `dataset_type`."""
        if isinstance(dataset_type, DatasetType) and dataset_type in DatasetType:
            return cls.__classes[dataset_type.name]()
        if isinstance(dataset_type, str) and dataset_type in DatasetType:
            return cls.__classes[dataset_type]()

        raise DatasetInvalidError("Dataset type %s not supported." % dataset_type)


DatasetManagerFactory.load_classes()

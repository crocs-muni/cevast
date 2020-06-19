"""This module contains DatasetManager factory implementation."""

from typing import Union
from cevast.dataset.dataset import DatasetType, DatasetInvalidError
import cevast.dataset.managers as managers


class DatasetManagerFactory:
    """Class implementing factory design patter for creation of DatasetManager objects."""

    __classes = {}

    @classmethod
    def __load_classes(cls):
        """
        Automatically initialize lookup dictionary with specialized DatasetManager classes.
        To be automatically identified, the specialized DatasetManager class must implement
        DatasetManager interface and put in "cevast.dataset.managers" package.
        """
        for manager_class in managers.DatasetManager.__subclasses__():
            if(hasattr(manager_class, 'dataset_type')):
                cls.__classes[manager_class.dataset_type] = manager_class

    @classmethod
    def create_manager(cls, dataset_type: Union[DatasetType, str]) -> managers.DatasetManager:
        """Instantiate a corresponding DatasetManager class based on `dataset_type`."""
        if not cls.__classes:
            cls.__load_classes()

        if isinstance(dataset_type, DatasetType) and dataset_type.name in cls.__classes:
            return cls.__classes[dataset_type.name]()
        if isinstance(dataset_type, str) and dataset_type in DatasetType.__members__ and dataset_type in cls.__classes:
            return cls.__classes[dataset_type]()

        raise DatasetInvalidError("Dataset type %s has no manager." % dataset_type)
 
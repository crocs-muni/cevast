"""This module contains DatasetManager factory implementation."""

from typing import Union
from .dataset import DatasetType, DatasetInvalidError
from . import managers as managers


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
            if hasattr(manager_class, 'dataset_type'):
                cls.__classes[str(manager_class.dataset_type)] = manager_class

    @classmethod
    def get_manager(cls, dataset_type: Union[DatasetType, str]) -> managers.DatasetManager:
        """Instantiate a corresponding DatasetManager class based on `dataset_type`."""
        if not cls.__classes:
            cls.__load_classes()

        # Validate and init dataset manager
        if not DatasetType.validate(dataset_type):
            raise DatasetInvalidError("Dataset type %s is not valid." % dataset_type)
        if str(dataset_type) not in cls.__classes:
            raise DatasetInvalidError("Dataset type %s has no manager." % dataset_type)

        return cls.__classes[str(dataset_type)]

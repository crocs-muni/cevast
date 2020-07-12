"""This module contains DatasetManager factory implementation."""

from typing import Union, Type
from .dataset import DatasetSource, DatasetInvalidError
from . import managers


class DatasetManagerFactory:
    """
    Factory class providing the specific DatasetManager class based on DatasetSource.

    .. IMPORTANT:: DatasetManager classes are registered automatically. To add a new
    specialized DatasetManager class, it must inherit DatasetManager and be placed
    in "cevast.dataset.managers" package.
    """

    __classes = {}

    @classmethod
    def __load_classes(cls):
        """
        Automatically initialize lookup dictionary with subclasses of DatasetManager class
        that are visible to the Python interpreter (obtained from `type.__subclasses__()`).
        """
        for manager_class in managers.DatasetManager.__subclasses__():
            if hasattr(manager_class, 'dataset_source'):
                cls.__classes[str(manager_class.dataset_source)] = manager_class

    @classmethod
    def get_manager(cls, dataset_source: Union[DatasetSource, str]) -> Type[managers.DatasetManager]:
        """Return a corresponding DatasetManager class based on `dataset_source`."""
        if not cls.__classes:
            cls.__load_classes()

        # Validate and init dataset manager
        if not DatasetSource.validate(dataset_source):
            raise DatasetInvalidError("Dataset source %s is not valid." % dataset_source)
        if str(dataset_source) not in cls.__classes:
            raise DatasetInvalidError("Dataset source %s has no manager." % dataset_source)

        return cls.__classes[str(dataset_source)]

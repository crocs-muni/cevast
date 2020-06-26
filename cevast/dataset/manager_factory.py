"""This module contains DatasetManager factory implementation."""

from typing import Union, Type
from .dataset import DatasetType, DatasetInvalidError
from . import managers


class DatasetManagerFactory:
    """
    Factory class providing the specific DatasetManager class based on DatasetType.

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
            if hasattr(manager_class, 'dataset_type'):
                cls.__classes[str(manager_class.dataset_type)] = manager_class

    @classmethod
    def get_manager(cls, dataset_type: Union[DatasetType, str]) -> Type[managers.DatasetManager]:
        """Return a corresponding DatasetManager class based on `dataset_type`."""
        if not cls.__classes:
            cls.__load_classes()

        # Validate and init dataset manager
        if not DatasetType.validate(dataset_type):
            raise DatasetInvalidError("Dataset type %s is not valid." % dataset_type)
        if str(dataset_type) not in cls.__classes:
            raise DatasetInvalidError("Dataset type %s has no manager." % dataset_type)

        return cls.__classes[str(dataset_type)]

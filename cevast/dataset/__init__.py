"""This package is a collection of tools for working with certificate datasets."""

__all__ = [
    'DatasetSource',
    'DatasetState',
    'Dataset',
    'DatasetRepository',
    'DatasetCollectionError',
    'DatasetInvalidError',
    'DatasetUnificationError',
    'DatasetManagerFactory',
    'DatasetManager',
    'DatasetManagerTask',
]
__version__ = '1.1'
__author__ = 'Radim Podola'

from .dataset import (
    DatasetSource,
    DatasetState,
    Dataset,
    DatasetRepository,
    DatasetCollectionError,
    DatasetInvalidError,
    DatasetUnificationError,
)
from .manager_factory import DatasetManagerFactory
from .managers import DatasetManagerTask, DatasetManager

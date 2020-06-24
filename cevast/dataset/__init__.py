"""This package is a collection of tools for working with certificate datasets."""

__all__ = [
    'DatasetType',
    'DatasetState',
    'Dataset',
    'DatasetRepository',
    'DatasetCollectionError',
    'DatasetInvalidError',
    'DatasetParsingError',
    'DatasetManagerFactory',
    'DatasetManager',
    'DatasetManagerTask',
]
__version__ = '0.1'
__author__ = 'Radim Podola'

from .dataset import (
    DatasetType,
    DatasetState,
    Dataset,
    DatasetRepository,
    DatasetCollectionError,
    DatasetInvalidError,
    DatasetParsingError,
)
from .manager_factory import DatasetManagerFactory
from .managers import DatasetManagerTask, DatasetManager
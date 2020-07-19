"""
This package provides certificate dataset managers.

Import and add your DatasetManager implementation into __all__ for identification by DatasetManagerFactory.
"""

__all__ = ['DatasetManager', 'DatasetManagerTask', 'RapidDatasetManager']
__author__ = 'Radim Podola'

from .manager import DatasetManager, DatasetManagerTask
from .rapid import RapidDatasetManager

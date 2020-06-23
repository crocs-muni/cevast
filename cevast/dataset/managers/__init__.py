"""
This package provides certificate dataset managers.

Import and add your DatasetManager implementation into __all__ for identification by DatasetManagerFactory.
"""

__all__ = ['DatasetManager', 'RapidDatasetManager']
__version__ = '0.1'
__author__ = 'Radim Podola'

from .manager import DatasetManager
from .rapid import RapidDatasetManager

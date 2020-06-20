"""
This module contains unit tests of cevast.dataset.collectors package.
"""

import unittest
from unittest.mock import patch, PropertyMock
from cevast.dataset.dataset import DatasetType, DatasetInvalidError


class TestRapidCollector(unittest.TestCase):
    """Unit test class of RapidCollector class"""
    # TODO

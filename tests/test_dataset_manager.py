"""
This module contains unit tests of cevast.dataset.managers package.
"""

import unittest
from typing import Union
from enum import IntEnum
from unittest.mock import patch, PropertyMock
from cevast.dataset.dataset import DatasetSource, DatasetInvalidError
from cevast.dataset.manager_factory import DatasetManagerFactory


class DummyDatasetSource(IntEnum):
    """Dummy DatasetSource ENUM used for Mock."""

    RAPIDOS = 1
    CENSYSOS = 2

    @classmethod
    def validate(cls, source: Union['DummyDatasetSource', str]) -> bool:
        """Validate DummyDatasetSource."""
        if isinstance(source, str):
            return source in cls.__members__
        if isinstance(source, cls):
            return source in cls
        return False

    def __str__(self):
        return str(self.name)


class DummyRapidosManager:  # pylint: disable=R0903
    """Dummy DatasetManager used for Mock."""


class DummyCensysosManager:  # pylint: disable=R0903
    """Dummy DatasetManager used for Mock."""


class TestDatasetManagerFactory(unittest.TestCase):
    """Unit test class of DatasetManagerFactory class"""

    @patch('cevast.dataset.manager_factory.DatasetSource', DummyDatasetSource)
    @patch.object(DatasetManagerFactory, '_DatasetManagerFactory__classes', new_callable=PropertyMock)
    def test_create_manager(self, mocked):
        """Test implementation of DatasetManagerFactory method create_manager."""
        # Test to select correct manager
        mocked.return_value = {"RAPIDOS": DummyRapidosManager}
        self.assertEqual(DatasetManagerFactory.get_manager(DummyDatasetSource.RAPIDOS), DummyRapidosManager)
        self.assertEqual(DatasetManagerFactory.get_manager('RAPIDOS'), DummyRapidosManager)
        self.assertRaises(DatasetInvalidError, DatasetManagerFactory.get_manager, DummyDatasetSource.CENSYSOS)
        mocked.return_value = {"CENSYSOS": DummyCensysosManager, "RAPIDOS": DummyRapidosManager}
        self.assertEqual(DatasetManagerFactory.get_manager(DummyDatasetSource.RAPIDOS), DummyRapidosManager)
        self.assertEqual(DatasetManagerFactory.get_manager('RAPIDOS'), DummyRapidosManager)
        self.assertEqual(DatasetManagerFactory.get_manager(DummyDatasetSource.CENSYSOS), DummyCensysosManager)
        self.assertEqual(DatasetManagerFactory.get_manager('CENSYSOS'), DummyCensysosManager)

        # Test without manager
        mocked.return_value = {"RAPIDOS": DummyRapidosManager}
        self.assertRaises(DatasetInvalidError, DatasetManagerFactory.get_manager, DummyDatasetSource.CENSYSOS)
        self.assertRaises(DatasetInvalidError, DatasetManagerFactory.get_manager, 'CENSYSOS')

        # Test if not instance or part of Enum
        self.assertRaises(DatasetInvalidError, DatasetManagerFactory.get_manager, DatasetSource.RAPID)
        self.assertRaises(DatasetInvalidError, DatasetManagerFactory.get_manager, None)
        mocked.return_value = {"VALID": DummyRapidosManager}
        self.assertRaises(DatasetInvalidError, DatasetManagerFactory.get_manager, "VALID")

"""
This module contains structures and classes logically related to a dataset.
"""

from enum import Enum


class DatasetType(Enum):
    RAPID = 1
    CENSYS = 2


class Dataset():
    """
    A Dataset instance represents a single dataset.

    Dataset is identified by its type and ID. ID usually represents
    the date when the dataset was collected. Furthermore, the instance
    holds all known information about the dataset and provide access to it.
    """
    _type: DatasetType
    # internal structure holding type based dataset information
    _data: dict


class DatasetRepository():
    """
    A DatasetRepository instance represents a dataset repository.

    Provides access to the repository and functions supporting a repository management.
    """

    def __init__(self, repository: str):

    def lists() -> list(Dataset):

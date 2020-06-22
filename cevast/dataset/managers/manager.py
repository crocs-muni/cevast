"""This module contains DatasetManager interface."""

from typing import Tuple
from abc import ABC, abstractmethod, abstractclassmethod
from cevast.dataset.dataset import DatasetType
from cevast.certdb import CertDB


class DatasetManager(ABC):
    """
    Abstract class representing DatasetManager interface that is providing operations with a certificate dataset.

    For Manager to operate, at least a repository path and date must be provided. Date works as an identifier
    of the dataset even though the date don't need to match exactly - the newest dataset by that date is identified.
    Additionally a port number might be used to more specify the dataset.

    DatasetManager offers running the operations independently or running a series of operations at once by `run` method
    (usefull for running operations that would be rather complex and/or long-lasting running separatelly).
    Running a series might also be more optimized.
    """

    @property
    @abstractclassmethod
    def dataset_type(cls) -> DatasetType:
        """
        Dataset type property used to identify a manager specification.
        """

    @abstractmethod
    def run(self, work_queue: list) -> bool:
        """
        Run a series of operations.
        `work_queue` is list composed of the required operations.
        """

    @abstractmethod
    def collect(self) -> Tuple[str]:
        """
        Collect a dataset.
        Return tuple of collected datasets (full paths).
        """

    @abstractmethod
    def analyse(self, methods: list = None) -> str:
        """
        Analyse a dataset.
        Return result as formatted string.
        """

    @abstractmethod
    def parse(self, certdb: CertDB) -> Tuple[str]:
        """
        Parse a dataset.
        Return tuple of parsed datasets (full paths).
        """

    @abstractmethod
    def validate(self, dataset_id: str, validation_cfg: dict) -> str:
        """
        Validate a dataset.
        Return result as formatted string.
        """

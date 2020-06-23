"""This module contains DatasetManager interface."""

from typing import Tuple, Optional
from abc import ABC, abstractmethod, abstractclassmethod
from enum import IntEnum
from cevast.dataset.dataset import DatasetType
from cevast.certdb import CertDB


class DatasetManagerTask(IntEnum):
    """Enumaration class of all supported tasks with a certificate dataset."""

    COLLECT = 1
    ANALYSE = 2
    PARSE = 3
    VALIDATE = 4


class DatasetManager(ABC):
    """
    An abstract DatasetManager class representing an interface that can be used to perform
    various tasks with a certificate dataset.

    For Manager to perform a task, a repository path and date must be provided. Date works
    as an identifier of the dataset even though the date don't need to match exactly
     - the newest dataset by that date is identified. Additionally a port number might be
    used to more specify the dataset.

    DatasetManager offers performing tasks independently or running a series of tasks at once
    by `run` method (usefull for performing tasks that would be rather complex and/or long-lasting
    running separatelly). Running a series might also be more optimized.
    """

    @property
    @abstractclassmethod
    def dataset_type(cls) -> DatasetType:
        """
        Dataset type property used to identify a manager specification.
        """

    @abstractmethod
    def run(self, task_pipline: Tuple[DatasetManagerTask], certdb: Optional[CertDB]) -> bool:
        """
        Run a series of tasks.
        `task_pipline` is list composed of the required tasks.
        `certdb` is CertDB instance to work with (if any task do not need it, might be None).
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

"""This module contains DatasetManager interface."""

import datetime
from typing import Tuple
from abc import ABC, abstractmethod, abstractclassmethod
from cevast.dataset.dataset import DatasetType


class DatasetManager(ABC):
    """
    Abstract class representing DatasetManager interface that is providing operations with a certificate dataset.

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
    def collect(self, date_range: Tuple[datetime.date, datetime.date] = None) -> str:
        """
        Collect a dataset in given date range.
        If `date_range` is None, a dataset with the latest date is collected.
        """

    @abstractmethod
    def analyse(self, dataset_id: str, methods: list = None) -> str:
        """
        Analyse a dataset.
        Return result as formatted string.
        """

    @abstractmethod
    def parse(self, dataset_id: str) -> None:
        """
        Parse a dataset.
        """

    @abstractmethod
    def validate(self, dataset_id: str, validation_cfg: dict) -> str:
        """
        Validate a dataset.
        Return result as formatted string.
        """

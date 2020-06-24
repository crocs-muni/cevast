"""This module contains DatasetManager interface."""

from typing import Tuple, Callable, Union
from datetime import datetime
from abc import ABC, abstractmethod, abstractclassmethod
from enum import IntEnum
from cevast.dataset.dataset import DatasetType, Dataset
from cevast.certdb import CertDB


class DatasetManagerTask(IntEnum):
    """Enumeration of DatasetManager Tasks"""

    COLLECT = 1
    ANALYSE = 2
    PARSE = 3
    VALIDATE = 4

    @classmethod
    def validate(cls, state: Union['DatasetManagerTask', str]) -> bool:
        """Validate DatasetManagerTask."""
        if isinstance(state, cls):
            return state in cls
        if isinstance(state, str):
            return state in cls.__members__
        return False

    def __str__(self):
        return str(self.name)


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
    def __init__(self, repository: str, date: datetime.date = datetime.today().date(),
                 ports: Tuple[str] = ('443',), cpu_cores: int = 1):
        """
        Initialize Manager.
        `repository` is dataset repository,
        'date' is date,
        'ports' is list of ports more specifying datasets,
        'cpu_cores' is maximum number of CPU cores that might be used.
        """

    @abstractmethod
    def run(self, task_pipline: Tuple[Tuple[DatasetManagerTask, dict]]) -> None:
        """
        Run a series of tasks.
        `task_pipline` is tuple composed of the required tasks in form of pairs ('task', 'cfg'), where:
            - 'task' is supported DatasetManagerTask,
            - 'cfg' is dictionary filled of parameters that will be passed to individual task methods.
        Caller function must ensure that 'cfg' parameters match task method's declaration.
        TODO dict is optional
        """

    @abstractmethod
    def collect(self, api_key: str = None) -> Tuple[Dataset]:
        """
        Collect a dataset.
        `api_key` is API access key that might be needed to retrieve datasets (depends on type implementation).
        Return tuple of collected Datasets.
        """

    @abstractmethod
    def analyse(self, methods: list = None) -> Tuple[Dataset]:
        """
        Analyse a dataset with given methods.
        Return tuple of analysed Datasets.
        """

    @abstractmethod
    def parse(self, certdb: CertDB) -> Tuple[Dataset]:
        """
        Parse a dataset.
        `certdb` is CertDB instance to work with (to insert parsed certificates to).
        Return tuple of parsed Datasets.
        """

    @abstractmethod
    def validate(self, certdb: CertDB, validator: Callable, validator_cfg: dict) -> Tuple[Dataset]:
        """
        Validate a dataset with given validor.
        `validator` is a validator callback function.
        `validator_cfg` is a validator config that will be passed to the function.

        Call to validator is performed like this: validator(cert_chain, validator_cfg)

        Return tuple of validated Datasets.
        """

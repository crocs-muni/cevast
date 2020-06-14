"""
This module contains structures and classes logically related to a certificate datasets.
"""

import os
import shutil
from typing import Union
from enum import IntEnum
from cevast.utils import directory_with_prefix


class DatasetInvalidError(ValueError):
    """Raised when the dataset has an invalid identifier."""


class DatasetType(IntEnum):
    """Enumaration class of all supported Dataset types."""

    RAPID = 1
    CENSYS = 2


class DatasetState(IntEnum):
    """Enumaration class of all supported Dataset states."""

    COLLECTED = 1  # Dataset was collected and is available in a raw format
    ANALYZED = 2   # Dataset was analyzed
    PARSED = 3     # Dataset was parsed to internal format, certificates were stored to CertDB
    VALIDATED = 4  # Dataset was already run through validation, result might be available


class DatasetPath:
    """
    Wrapper around a single dataset's location providing a path to the dataset on the filesystem.

    Dataset is identified by its type and ID (static identifiers). ID usually represents the date
    (or date range) when the dataset was created (certificates were collected and added to the dataset).

    Furthermore, the dataset has its state (dynamic identifier) that is the last part of its complete
    identification. Each dataset can be found at 1-N of the following generalized states:
        - COLLECTED
        - ANALYZED
        - PARSED
        - VALIDATED
    """

    EXTENSION = 'gz'

    def __init__(self, repository: str, dataset_type: Union[DatasetType, str], dataset_id: str):
        """Initialize the static identifiers"""
        # Validate and init dataset repository
        if not os.path.exists(repository):
            raise FileNotFoundError("Repository %s not found" % repository)
        self.__repository = os.path.abspath(repository)
        # Validate and init dataset type
        if isinstance(dataset_type, DatasetType) and dataset_type in DatasetType:
            self.__dataset_type = dataset_type.name
        elif isinstance(dataset_type, str) and dataset_type in DatasetType:
            self.__dataset_type = dataset_type
        else:
            raise DatasetInvalidError("Dataset type %s not supported." % dataset_type)
        # Dataset ID
        self.__dataset_id = dataset_id

    def get(self, state: Union[DatasetState, str], check_if_exists: bool = True) -> str:
        """
        Return path to the dataset in given state.
        Return None if `check_if_exists` and path does not exist.
        """
        # Validate dataset state
        if isinstance(state, DatasetState) and state in DatasetState:
            self.__dataset_type = state.name
        elif isinstance(state, str) and state in DatasetState:
            self.__dataset_type = state
        else:
            raise DatasetInvalidError("State %s not valid" % state)

        path = os.path.join(self.__repository, self.__dataset_type, state.name)
        if check_if_exists and not os.path.exists(path):
            return None
        return path

    def get_full(self, state: Union[DatasetState, str], suffix: str = '', check_if_exists: bool = True) -> str:
        """
        Return full path to the dataset file in given state including custome suffix.
        Return None if `check_if_exists` and file does not exist.
        """
        filename = "{}_{}.{}".format(self.__dataset_id, suffix, self.EXTENSION)
        path = self.get(state, check_if_exists)
        if check_if_exists and not (path and os.path.exists(path)):
            return None
        return os.path.join(path, filename)

    def delete(self, state: Union[DatasetState, str]) -> None:
        """Delete the dataset of given state from the repository."""
        for file in directory_with_prefix(self.get(state, False), self.__dataset_id):
            os.remove(file)

    def purge(self) -> None:
        """Delete all datasets of specified type from the repository."""
        shutil.rmtree(os.path.join(self.__repository, self.__dataset_type), ignore_errors=True)

    def exists(self, state: Union[DatasetState, str]) -> bool:
        """Test if the dataset exists in given state."""
        for _ in directory_with_prefix(self.get(state, False), self.__dataset_id):
            return True
        return False

    def exists_any(self) -> bool:
        """Test if the dataset exists in any state."""
        for state in DatasetState:
            if self.exists(state):
                return True
        return False

    def move(self, state: Union[DatasetState, str], source: str, prefix_id: bool = True) -> None:
        """
        Move the source file to the repository of the dataset of given state.
        If `prefix_id` is true, then prefix "dataset_id_" is added to the file.
        """
        if os.path.exists(source):
            path = self.get(state, False)
            filename = os.path.basename(source)
            if prefix_id:
                filename = "{}_{}".format(self.__dataset_id, filename)
            if not os.path.exists(path):
                os.makedirs(path)
            shutil.move(os.path.abspath(source), os.path.join(path, filename))


class DatasetRepository:
    """
    Wrapper around the whole dataset repository providing overview and abstraction of the storage system.
    """

    def __init__(self, repository: str):
        self.repo = os.path.abspath(repository)

    def dumps(self, dataset_type: DatasetType = None, state: DatasetState = None, dataset_id: str = '') -> str:
        """
        Return string representation of the specified dataset repository.
        The parameters represent the output filter options.
        """
        repo = self.get(dataset_type, state, dataset_id)
        repo_str = ''
        for d_type in repo:
            for d_state in d_type:
                for dataset in d_state:
                    repo_str += "{}:{}:{}\n".format(d_type, d_state, dataset)
        return repo_str

    def dump(self, dataset_type: DatasetType = None, state: DatasetState = None, dataset_id: str = '') -> None:
        """
        Print string representation of the specified dataset repository to the STDOUT.
        The parameters represent the output filter options.
        """
        print(self.dumps(dataset_type, state, dataset_id))

    def get(self, dataset_type: DatasetType = None, state: DatasetState = None, dataset_id: str = '') -> dict:
        """
        Return dictionary representation of the specified dataset repository.
        The parameters represent the output filter options.
        """

        def get_state(state: DatasetState) -> tuple:
            path = dataset_path.get(state)
            if not path:
                return tuple()
            return tuple(directory_with_prefix(path, dataset_id, True))

        def get_type() -> dict:
            ret_type = {}
            if state:
                ret_type[state.name] = get_state(state)
            else:
                for d_state in DatasetState:
                    ret_type[d_state.name] = get_state(d_state)
            return ret_type

        ret_repo = {}

        if dataset_type:
            dataset_path = DatasetPath(self.repo, dataset_type, dataset_id)
            ret_repo[dataset_type] = get_type()
        else:
            for d_type in DatasetType:
                dataset_path = DatasetPath(self.repo, d_type, dataset_id)
                ret_repo[d_type] = get_type()

        return ret_repo

    def __str__(self):
        return self.dumps()

"""
This module contains structures and classes logically related to a certificate datasets.
"""

import os
import logging
import shutil
import re
from typing import Union, Tuple
from enum import IntEnum
from cevast.utils import directory_with_prefix


log = logging.getLogger(__name__)


class DatasetCollectionError(ValueError):
    """Raised when dataset collection fails."""


class DatasetUnificationError(ValueError):
    """Raised when dataset unification fails."""


class DatasetInvalidError(ValueError):
    """Raised when the dataset has an invalid identifier."""


class DatasetType(IntEnum):
    """Enumaration class of all supported Dataset types."""

    RAPID = 1
    CENSYS = 2

    @classmethod
    def validate(cls, dataset_type: Union['DatasetType', str]) -> bool:
        """Validate DatasetType."""
        if isinstance(dataset_type, cls):
            return dataset_type in cls
        if isinstance(dataset_type, str):
            return dataset_type in cls.__members__
        return False

    def __str__(self):
        return str(self.name)


class DatasetState(IntEnum):
    """Enumaration class of all supported Dataset states."""

    COLLECTED = 1  # Dataset was collected and is available in a raw format
    FILTERED = 2  # Dataset was fitered
    UNIFIED = 3  # Dataset was unified to internal format, certificates were stored to CertDB
    ANALYSED = 4  # Dataset was already run through analysis, result might be available

    @classmethod
    def validate(cls, state: Union['DatasetState', str]) -> bool:
        """Validate DatasetState."""
        if isinstance(state, cls):
            return state in cls
        if isinstance(state, str):
            return state in cls.__members__
        return False

    def __str__(self):
        return str(self.name)


class Dataset:
    """
    Class representing a single dataset and providing an interface to the dataset on the filesystem.

    Dataset is identified by `dataset_type`, `state` and filename where filename consists of mandatory
    `date_id`, `port` number and optional suffix. date_id represents the date (or date range) when
    the dataset was created (certificates were collected and added to the dataset) and its string in fomat
    "YYYYMMDD", port is application port on which was the data collceted, suffix can specify the dataset
    in various ways and is used to distinguish the files internally. `dataset_type`, `date_id` and `port`
    are static identifiers provided upon object initialization.

    The dataset state is dynamic identifier that is the last part of its complete identification at the time.
    Each dataset can be found at 1-N of the following generalized states:
        - COLLECTED
        - ANALYZED
        - UNIFIED
        - ANALYSED

    Full Dataset path template: {repository}/{type}/{state}/{date_id}[_{port}][_suffix].{extension}
    """

    def __init__(self, repository: str, dataset_type: Union[DatasetType, str],
                 date_id: str, port: Union[str, int], extension: str = 'gz'):
        """Initialize the static identifiers"""
        # Validate and init dataset repository
        if not os.path.exists(repository):
            raise DatasetInvalidError("Repository %s not found" % repository)
        self._repository = os.path.abspath(repository)
        # Validate and init dataset type
        if not DatasetType.validate(dataset_type):
            raise DatasetInvalidError("Dataset type %s is not valid." % dataset_type)
        self._dataset_type = str(dataset_type)
        self._date_id = date_id
        self._port = str(port) if port is not None else ''
        self._extension = extension

    @property
    def dataset_type(self) -> str:
        """Get the Dataset Type."""
        return self._dataset_type

    @property
    def date(self) -> str:
        """Get the DATE ID."""
        return self._date_id

    @property
    def port(self) -> str:
        """Get the PORT."""
        return self._port

    @property
    def extension(self) -> str:
        """Get the extension."""
        return self._extension

    @property
    def static_filename(self) -> str:
        """Get the static part of dataset filename."""
        return Dataset.format_filename(self._date_id, self._port)

    @classmethod
    def from_full_path(cls, path: str) -> 'Dataset':
        """
        Initialize Dataset object from the given path,
        or return None if object cannot be initialized.
        """
        template = r"^(?P<repo>\S+)[/\\](?P<type>\S+)[/\\](?P<state>\S+)[/\\](?P<date>\d{8})(_(?P<port>\d+))?(_\S+)?\.(?P<ext>\S+)$"
        match = re.match(template, path)
        if not match:
            return None
        try:
            return cls(repository=match.group('repo'),
                       dataset_type=match.group('type'),
                       date_id=match.group('date'),
                       port=match.group('port'),
                       extension=match.group('ext'),
                       )
        except DatasetInvalidError:
            log.exception("Cannot initialize Dataset class from the given path.")
            return None

    @staticmethod
    def format_filename(date: str, port: str = '', suffix: str = '') -> str:
        """Format dataset filename."""
        if port and suffix:
            return "{}_{}_{}".format(date, port, suffix)
        if port or suffix:
            return "{}_{}".format(date, port or suffix)
        return date

    def path(self, state: Union[DatasetState, str], physically: bool = True) -> str:
        """
        Assemble and return path to the dataset in given state.
        If `physically` flag is set and and path does not exist, create it.
        """
        # Validate dataset state
        if not DatasetState.validate(state):
            raise DatasetInvalidError("Dataset state %s is not valid." % state)

        path = os.path.join(self._repository, self._dataset_type, str(state))

        if physically and not os.path.exists(path):
            log.info("Path <%s> does not exist yet, will be created.", path)
            os.makedirs(path)
        return path

    def full_path(self, state: Union[DatasetState, str], suffix: str = '',
                  check_if_exists: bool = False, physically: bool = False) -> str:
        """
        Assemble and return full path to the dataset file in given state including custome suffix.
        Return None if `check_if_exists` and file does not exist.
        """
        filename = Dataset.format_filename(self._date_id, self._port, suffix)
        path = os.path.join(self.path(state, physically), filename + '.' + self._extension)
        if check_if_exists and not os.path.exists(path):
            return None
        return path

    def delete(self, state: Union[DatasetState, str]) -> None:
        """Delete the dataset of given state from the repository."""
        path = self.path(state, False)
        if not os.path.exists(path):
            return
        for file in directory_with_prefix(path, Dataset.format_filename(self._date_id, self._port)):
            log.debug("Will delete dataset <%s>.", file)
            os.remove(file)
        if not os.listdir(path):
            log.info("No more datasets in state <%s>, directory will be deleted.", state)
            os.rmdir(path)

    def purge(self) -> None:
        """Delete all datasets of specified type from the repository."""
        shutil.rmtree(os.path.join(self._repository, self._dataset_type), ignore_errors=True)

    def get(self, state: Union[DatasetState, str], suffix: str = '', full_path: bool = False) -> Tuple[str]:
        """Return all datasets stored in the dataset repository matching the paramaters."""
        filename = Dataset.format_filename(self._date_id, self._port, suffix)
        path = self.path(state, False)
        return tuple(directory_with_prefix(path, filename, not full_path))

    def exists(self, state: Union[DatasetState, str]) -> bool:
        """Test if the dataset exists in given state."""
        path = self.path(state, False)
        for _ in directory_with_prefix(path, Dataset.format_filename(self._date_id, self._port)):
            return True
        return False

    def exists_any(self) -> bool:
        """Test if any dataset (of the specified type) exists."""
        for state in DatasetState:
            if self.exists(state):
                return True
        return False

    def move(self, state: Union[DatasetState, str], source: str, format_name: bool = True) -> None:
        """
        Move the source file to the repository of the dataset of given state.
        If `format_name` is true, then name is formatted.
        """
        if os.path.exists(source):
            path = self.path(state)
            filename = os.path.basename(source)
            if format_name:
                filename = Dataset.format_filename(self._date_id, self._port, filename)
            shutil.move(os.path.abspath(source), os.path.join(path, filename))

    def __str__(self):
        return os.path.join(self._repository, self._dataset_type, "{}", Dataset.format_filename(self._date_id, self._port))

    def __repr__(self):
        return "<%s.%s dataset_type=%s, date_id=%s, port=%s>" % (
            self.__class__.__module__,
            self.__class__.__qualname__,
            self._dataset_type,
            self._date_id,
            self._port,
        )

    def __eq__(self, other):
        if not isinstance(other, Dataset):
            # don't attempt to compare against unrelated types
            return NotImplemented

        return self._port == other.port and self._date_id == other.date and self._dataset_type == other.dataset_type

    def __hash__(self):
        return hash((self._port, self._date_id, self._dataset_type))


class DatasetRepository:
    """
    Wrapper around the whole dataset repository providing overview and abstraction of the storage system.
    """

    def __init__(self, repository: str):
        if repository and os.path.exists(repository):
            self.repository = os.path.abspath(repository)
        else:
            raise FileNotFoundError("Dataset Repository %s not found." % repository)

    def dumps(self, dataset_type: Union[DatasetType, str] = None,
              state: Union[DatasetState, str] = None, dataset_id: str = '') -> str:
        """
        Return string representation of the specified dataset repository.
        The parameters represent the output filter options.
        """
        repo = self.get(dataset_type, state, dataset_id)
        repo_str = ''
        for d_type, d_states in repo.items():
            repo_str += '{:<8}: '.format(d_type)
            first_state = True

            for d_state, d_datasets in d_states.items():
                if first_state:
                    first_state = False
                else:
                    repo_str += " " * 10
                repo_str += "{:<10}: ".format(d_state)

                first_dataset = True
                for dataset in d_datasets:
                    if first_dataset:
                        repo_str += dataset + "\n"
                        first_dataset = False
                    else:
                        repo_str += " " * 22 + dataset + "\n"

        return repo_str

    def dump(self, dataset_type: Union[DatasetType, str] = None,
             state: Union[DatasetState, str] = None, dataset_id: str = '') -> None:
        """
        Print string representation of the specified dataset repository to the STDOUT.
        The parameters represent the output filter options.
        """
        print(self.dumps(dataset_type, state, dataset_id))

    def get(self, dataset_type: Union[DatasetType, str] = None,
            state: Union[DatasetState, str] = None, dataset_id: str = '') -> dict:
        """
        Return dictionary representation of the specified dataset repository.
        The parameters represent the output filter options.
        """

        def get_type() -> dict:
            ret_type = {}
            states = [state] if state else DatasetState
            # Iterate through filtered states and get its datasets
            for d_state in states:
                ret_state = dataset_path.get(d_state)
                if ret_state:
                    ret_type[str(d_state)] = ret_state
            return ret_type

        # Validate dataset type
        if dataset_type and not DatasetType.validate(dataset_type):
            raise DatasetInvalidError("Dataset type %s is not valid." % dataset_type)
        # Validate dataset state
        if state and not DatasetState.validate(state):
            raise DatasetInvalidError("Dataset state %s is not valid." % state)

        ret_repo = {}
        types = [dataset_type] if dataset_type else DatasetType
        # Iterate through filtered types and get its states
        for d_type in types:
            dataset_path = Dataset(self.repository, d_type, dataset_id, None)
            ret_type = get_type()
            if ret_type:
                ret_repo[str(d_type)] = ret_type

        return ret_repo

    def __str__(self):
        return self.dumps()

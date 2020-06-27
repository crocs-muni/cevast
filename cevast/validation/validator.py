"""This module contains CertValidator interface."""

from typing import List
from abc import ABC, abstractmethod


class CertValidator(ABC):
    """
    An abstract CertValidator class an interface design that provides possibility to perform
    various validation tasks with certificates. Design aims for using parallelism but can also be
    used as a single thread process. With such design one can use either multithreading or
    multiprocessing to run validation tasks, depends what performance insufficiency is targeted
    (I/O vs CPU operating costs).

    Class implements mandatory methods of context manager interface so can be (and is recommended)
    to use with `with` statement. This way one can be sure that pool is safely cleaned-up.

    `output_file` is a file where results will be written,
    `processes` is maximum number of additional workers (threads or processes) [0=single thread],
    `kwargs` are optional key arguments special to concrete implementation.

    TODO pass list of actual validation methods that should be run?
    """

    def __init__(self, output_file: str, processes: int, **kwargs):
        """Initialize CertValidator."""

    @abstractmethod
    def schedule(self, host: str, chain: List[str]) -> None:
        """Enqueue host and certificate chain for validation."""

    @abstractmethod
    def done(self) -> None:
        """
        Indicate that no more data will be scheduled to validate and clean up context.
        If multithreading/multiprocessing is used, this function will close-up the pool and wait
        for all workers to finish.
        """

    @abstractmethod
    def __enter__(self):
        """Return self."""

    @abstractmethod
    def __exit__(self, exc_type, exc_value, traceback):
        """Safely clean-up context."""

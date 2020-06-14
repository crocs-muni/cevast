"""This module contains DatasetManager interface."""

from abc import ABC, abstractmethod


class DatasetManager(ABC):
    """
    Abstract class representing DatasetManager interface.

    DatasetManager is a class providing operations with dataset...
    """

    @abstractmethod
    def run(self, work_queue: list) -> bool:
        """
        Run work queue.
        """

    @abstractmethod
    def collect(self) -> str:
        """
        Collect a dataset.
        """

    @abstractmethod
    def parse(self) -> str:
        """
        Parse a dataset.
        """

"""This module contains DatasetManager interface implementation of RAPID dataset type."""

from cevast.dataset.manager import DatasetManager


class RapidDatasetManager(DatasetManager):
    """DatasetManager interface implementation of RAPID dataset type."""

    C_CERT_NAME_SUFFIX = '-certs.gz'
    C_HOSTS_NAME_SUFFIX = '-hosts.gz'

    def __init__(self, repository: str):
        self.__repository = repository

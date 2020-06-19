"""This module contains DatasetManager interface implementation of RAPID dataset type."""

from cevast.dataset.managers.manager import DatasetManager
from cevast.dataset.dataset import DatasetType


class RapidDatasetManager(DatasetManager):
    """DatasetManager interface implementation of RAPID dataset type."""

    C_CERT_NAME_SUFFIX = '-certs.gz'
    C_HOSTS_NAME_SUFFIX = '-hosts.gz'

    dataset_type = DatasetType.RAPID.name

    def __init__(self, repository: str, cert_db: object):
        self.__repository = repository

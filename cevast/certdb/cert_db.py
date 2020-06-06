"""
This module provides interface of CertDB class

    CertDB is a database of X.509 certificates implemented as transaction
    processing system with a common API (insert, remove, commit, rollback).

    Each certificate is uniquelly identified by its ID (fingerprint).
    A certificate is internally handled as string -> PEM format expected.

    It is expected that certificate's ID is uniquely matching the certificate in the world
    as a fingerprint should be used as ID. Therefore, inserting different certificate
    under same ID does not lead to rewriting the original certificate. To replace
    a specific certificate, it must first be deleted and then re-inserted - it can
    be performed in a single transaction as long as the sequence is preserved.

    Be aware that DELETE metod is deleting not persisted certificate immediatelly but
    the persisted one remains untill the transaction is committed. Therefore, methods like
    GET/EXPORT/EXISTS will not find not persisted deleted certificate.
"""

from abc import ABC, abstractmethod
from typing import Tuple

__author__ = 'Radim Podola'


class CertNotAvailableError(Exception):
    """Raised when the certificate is not available in database"""


class CertInvalidError(ValueError):
    """Raised when the certificate has an invalid identifier or a structure"""


class CertDBReadOnly(ABC):
    """Abstract class representing read-only CertDB interface."""

    @abstractmethod
    def get(self, cert_id: str) -> str:
        """
        Retrieve a certificate from the database.

        'cert_id' is the certificate identifier.
        Certificate is returned in a PEM format.
        Raise CertNotAvailableError if the certificate is not found.
        """

    @abstractmethod
    def export(self, cert_id: str, target_dir: str, copy_if_exists: bool = True) -> str:
        """
        Export a certificate from the database and saves it as a PEM file.

        'cert_id' is the certificate identifier,
        'target_dir' is the target directory.
        If 'copy_if_exists' is false and file already exists (e.g. temporary in open transaction),
        the file is not copied to the target directory, instead the existing file path is returned.

        Full path of the certificate file is returned.
        Raise CertNotAvailableError if the certificate is not found.
        """

    @abstractmethod
    def exists(self, cert_id: str) -> bool:
        """
        Test whether a certificate exists in the database.

        'cert_id' is the certificate cert_identifier.
        """

    @abstractmethod
    def exists_all(self, cert_ids: list) -> bool:
        """
        Test that all certificates exist in the database.

        'cert_ids' is a list of certificate identifiers.
        """


# TODO add PURGE method for completely deleteting the storage
class CertDB(CertDBReadOnly):
    """Abstract class representing CertDB interface."""

    @abstractmethod
    def insert(self, cert_id: str, cert: str) -> None:
        """
        Insert the certificate to the database under 'cert_id' identifier.

        Inserted certificate is not persisted immediatelly but
        remains in current open transaction untill commit or rollback.
        A expected format of certificate is PEM.
        """

    @abstractmethod
    def delete(self, cert_id: str):
        """
        Delete the certificate from the database.

        Persisted certificate is not immediatelly deleted but
        remains untill commit or rollback. Certificate inserted
        in the current transaction is deleted immediatelly.
        """

    @abstractmethod
    def rollback(self) -> None:
        """
        Revert the changes made by the current transaction.

        All inserted certificates waiting to persist are removed.
        All deleted certificates in the current transaction stay untouched.
        """

    @abstractmethod
    def commit(self, cores=1) -> Tuple[int, int]:
        """
        Apply the changes made by the current transaction.

        All inserted certificates waiting to persist are persisted.
        All deleted certificates in the current transaction are permanently removed.
        Return tuple of numbers (number of inserted; number of deleted)
        """

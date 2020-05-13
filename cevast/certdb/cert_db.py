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

__author__ = 'Radim Podola'


class CertNotAvailableError(Exception):
    """Raised when the certificate is not available in database"""
    pass


class CertInvalidError(Exception):
    """Raised when the certificate has an invalid identifier or a structure"""
    pass


class CertDBReadOnly(ABC):
    """Abstract class representing read-only CertDB interface."""

    @abstractmethod
    def get(self, id: str) -> str:
        """Retrieve the certificate from the database.
           'id' is the certificate identifier.
           Certificate is returned in a PEM format.
           Raise CertNotAvailableError if the certificate is not found.
        """
        pass

    @abstractmethod
    def export(self, id: str, target_dir: str) -> str:
        """Export the certificate from the database and
           saves it as a PEM file in the 'target_dir' directory.
           'id' is the certificate identifier.
           Full path of the certificate file is returned.
           Raise CertNotAvailableError if the certificate is not found.
        """
        pass

    @abstractmethod
    def exists(self, id: str) -> bool:
        """Test whether a certificate exists in the database.
           'id' is the certificate identifier.
        """
        pass

    @abstractmethod
    def exists_all(self, ids: list) -> bool:
        """Test that all certificates exist in the database.
           'ids' is a list of certificate identifiers.
        """
        pass


class CertDB(CertDBReadOnly):
    """Abstract class representing CertDB interface."""

    @abstractmethod
    def insert(self, id: str, cert: str) -> None:
        """Insert the certificate to the database under 'id' identifier.
           Inserted certificate is not persisted immediatelly but
           remains in current open transaction untill commit or rollback.
           A expected format of certificate is PEM.
        """
        pass

    @abstractmethod
    def rollback(self) -> None:
        """Revert the changes made by the current transaction.
           All inserted certificates waiting to persist are removed.
           All deleted certificates in the current transaction stay untouched.
        """
        pass

    @abstractmethod
    def commit(self, cores=1) -> None:
        """Apply the changes made by the current transaction.
           All inserted certificates waiting to persist are persisted.
           All deleted certificates in the current transaction are permanently removed.
        """
        pass

    @abstractmethod
    def delete(self, id: str):
        """Delete the certificate from the database.
           Persisted certificate is not immediatelly deleted but
           remains untill commit or rollback. Certificate inserted
           in the current transaction is deleted immediatelly.
        """
        pass

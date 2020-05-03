"""
This module provides interface of CertDB class

    CertDB is a database of X.509 certificates.
    A certificate is uniquely identified by its SHA-1 fingerprint
"""

from abc import ABC, abstractmethod

__author__ = 'Radim Podola'


class CertNotAvailableError(Exception):
    """Raised when the certificate is not available in database"""
    pass


class InvalidCertError(Exception):
    """Raised when the certificate has an invalid identifier or a structure"""
    pass


class CertDBReadOnly(ABC):

    @abstractmethod
    def get(self, sha: str):
        pass

    @abstractmethod
    def exists(self, sha: str):
        pass


class CertDB(CertDBReadOnly):

    @abstractmethod
    def insert(self, sha: str, cert: str):
        pass

    @abstractmethod
    def rollback(self):
        pass

    @abstractmethod
    def commit(self, cores=1):
        pass

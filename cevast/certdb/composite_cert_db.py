"""
This module contains implementation of CompositeCertDB.

    CompositeCertDB is an implementation of CertDB component following Composite design pattern.

    With a single instance of CompositeCertDB one can manipulate a group CertDB instances together
    via unified interface. Mixed instances of CertDB and CertDBReadOnly are possible.
"""

import logging
from typing import List, Tuple, Union
from cevast.certdb.cert_db import (
    CertDB,
    CertDBReadOnly,
    CertNotAvailableError,
)

__author__ = 'Radim Podola'

log = logging.getLogger(__name__)
# TODO make performance test


class CompositeCertDBReadOnly(CertDBReadOnly):
    """
    Composite manager of CertDBReadOnly components.
    """

    def __init__(self):
        log.info('Initializing CompositeCertDBReadOnly composite manager...')
        # List managing all registered CertDBReadOnly components
        self._children: List(CertDBReadOnly) = []

    def register(self, certdb: CertDBReadOnly) -> None:
        """Add component object to the composite manager."""
        if certdb not in self._children:
            self._children.append(certdb)

    def unregister(self, certdb: CertDBReadOnly) -> None:
        """Remove component object from the composite manager."""
        try:
            self._children.remove(certdb)
        except ValueError:
            pass

    def is_registered(self, certdb: CertDBReadOnly) -> bool:
        """Test if component object is registered in the composite manager."""
        return certdb in self._children

    def get(self, cert_id: str) -> str:
        for child in self._children:
            try:
                return child.get(cert_id)
            except CertNotAvailableError:
                pass
        raise CertNotAvailableError

    def export(self, cert_id: str, target_dir: str, copy_if_exists: bool = True) -> str:
        for child in self._children:
            try:
                return child.export(cert_id, target_dir, copy_if_exists)
            except CertNotAvailableError:
                pass
        raise CertNotAvailableError

    def exists(self, cert_id: str) -> bool:
        for child in self._children:
            if child.exists(cert_id):
                return True
        return False

    def exists_all(self, cert_ids: list) -> bool:
        for cert_id in cert_ids:
            for child in self._children:
                if child.exists(cert_id):
                    break
            else:
                return False  # only executed if the inner loop did NOT break
            pass  # only executed if the inner loop DID break

        return True


class CompositeCertDB(CertDB, CompositeCertDBReadOnly):
    """
    Composite manager of CertDB components.
    """

    def __init__(self):  # pylint: disable=W0231
        log.info('Initializing CompositeCertDB composite manager...')
        # List managing all registered CertDB components
        self._children: List(Union[CertDB, CertDBReadOnly]) = []
        # List managing all registered CertDBReadOnly components
        self.__io_allowed: List(CertDB) = []

    def register(self, certdb: Union[CertDB, CertDBReadOnly]) -> None:
        if certdb not in self._children:
            self._children.append(certdb)
            if isinstance(certdb, CertDB):
                self.__io_allowed.append(certdb)

    def unregister(self, certdb: Union[CertDB, CertDBReadOnly]) -> None:
        try:
            self._children.remove(certdb)
        except ValueError:
            pass
        try:
            self.__io_allowed.remove(certdb)
        except ValueError:
            pass

    def insert(self, cert_id: str, cert: str) -> None:
        for child in self.__io_allowed:
            child.insert(cert_id, cert)

    def delete(self, cert_id: str) -> None:
        for child in self.__io_allowed:
            child.delete(cert_id)

    def rollback(self) -> None:
        for child in self.__io_allowed:
            child.rollback()

    def commit(self) -> Tuple[int, int]:
        cnt_inserted, cnt_deleted = 0, 0
        for child in self.__io_allowed:
            cnt_inserted, cnt_deleted = child.commit()
        return cnt_inserted, cnt_deleted

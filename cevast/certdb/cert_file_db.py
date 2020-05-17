"""
This module contains implementation of CertFileDB

    CertFileDB is a simple local database that uses files
    and a file system properties as a storage mechanism.

Storage structure on the file system:
storage                  - path to the storage given as an initial parameter to CertFileDB
    - certs/             - hierarchy of certificate blocks (group of certificates with equal prefix)
        - id[2]/         - first 2 characters of certificate ID (fingerprint) make block (e.g. 1a/)
            - id[4].zip  - first 4 characters of certificate ID (fingerprint) (e.g. 1a9f.zip)
            - ...
        - ...
        - .CertFileDB    - CertFileDB storage metafile
"""

import os
import shutil
import logging
from zipfile import ZipFile, ZIP_DEFLATED
from cevast.utils import make_PEM_filename
from cevast.certdb.cert_db import (
    CertDB,
    CertDBReadOnly,
    CertNotAvailableError,
    CertInvalidError,
)

__author__ = 'Radim Podola'

log = logging.getLogger(__name__)

# TODO add storage certFileDB metafile with:
# - structure_level
# - compression method/level
# - number of cer managed
# - open transaction Flag - will be set by INSERT/REMOVE/ROLLBACK/COMMIT -> OpenTransaction/CloseTransaction decorator ??
# - maybe inetrnal strucure PEM vs DES,...
# TODO add structure level ? higher level for more records, 0 level for common pool ?
# TODO add cache of managed certs to improve lookup performance -> exists, etc...
#  - insert and True Exists will add to cache
#  - remove and rollback will remove from cache
# TODO remove certs/ block


class CertFileDBReadOnly(CertDBReadOnly):
    """
    CertDBReadOnly interface implementation which uses files
    and a file system properties as a storage mechanism.
    """

    CERT_STORAGE_NAME = 'certs'

    def __init__(self, storage: str):
        # Init attributes
        log.info('Initializing... %s at storage %s', self.__class__.__name__, storage)
        self.storage = os.path.abspath(storage)
        self._transaction: set = set()
        self._delete: set = set()
        # Init certificate storage location
        self._cert_storage = os.path.join(self.storage, self.CERT_STORAGE_NAME)
        log.debug('cert storage: %s', self._cert_storage)

        if not os.path.exists(self._cert_storage):
            raise ValueError('Storage location does not exists')

    def get(self, cert_id: str) -> str:
        loc = self._get_cert_location(cert_id)
        filename = make_PEM_filename(cert_id)
        # Check if certificate exists as a file (in case of open transaction)
        if self._transaction:
            cert_file = os.path.join(loc, filename)
            try:
                with open(cert_file, 'r') as source:
                    log.debug('<%s> found in open transaction', cert_file)
                    return source.read()
            except IOError:
                pass
        # Check if certificate exists in a zipfile
        try:
            zip_file = loc + '.zip'
            with ZipFile(zip_file, 'r', ZIP_DEFLATED) as z_obj:
                with z_obj.open(filename) as cert:
                    log.debug('<%s> found persisted in zip <%s>', filename, zip_file)
                    return cert.read().decode('utf-8')
        except (KeyError, FileNotFoundError):
            pass

        log.info('<%s> not found', filename)
        raise CertNotAvailableError(cert_id)

    def export(self, cert_id: str, target_dir: str) -> str:
        loc = self._get_cert_location(cert_id)
        filename = make_PEM_filename(cert_id)
        # Check if certificate exists as a file (in case of open transaction)
        if self._transaction:
            cert_src_file = os.path.join(loc, filename)
            cert_trg_file = os.path.join(target_dir, filename)
            if os.path.exists(cert_src_file):
                shutil.copyfile(cert_src_file, cert_trg_file)
                log.debug('<%s> found in open transaction', cert_src_file)
                return cert_trg_file
        # Check if certificate exists in a zipfile
        try:
            zip_file = loc + '.zip'
            with ZipFile(zip_file, 'r', ZIP_DEFLATED) as z_obj:
                z_obj.extract(filename, target_dir)
                log.debug('<%s> found persisted in zip <%s>', filename, zip_file)
                return os.path.join(target_dir, filename)
        except (KeyError, FileNotFoundError):
            pass

        log.info('<%s> not found', filename)
        raise CertNotAvailableError(cert_id)

    def exists(self, cert_id: str) -> bool:
        loc = self._get_cert_location(cert_id)
        cert_filename = make_PEM_filename(cert_id)
        # Check if certificate exists as a file (in case of open transaction)
        if self._transaction:
            cert_file = os.path.join(loc, cert_filename)
            if os.path.exists(cert_file):
                log.debug('<%s> exists in open transaction', cert_file)
                return True
        # Check if certificate exists in a zipfile
        try:
            zip_file = loc + '.zip'
            with ZipFile(zip_file, 'r', ZIP_DEFLATED) as z_obj:
                z_obj.getinfo(cert_filename)
                log.debug('<%s> exists persisted <%s>', cert_id, zip_file)
                return True
        except (KeyError, FileNotFoundError):
            pass

        log.debug('<%s> does not exist', cert_id)
        return False

    def exists_all(self, cert_ids: list) -> bool:
        for cert_id in cert_ids:
            if not self.exists(cert_id):
                return False

        return True

    def _get_cert_location(self, cert_id: str) -> str:
        return os.path.join(self._cert_storage, cert_id[:2], cert_id[:4])


class CertFileDB(CertDB, CertFileDBReadOnly):
    """
    CertDB interface implementation which uses files
    and a file system properties as a storage mechanism.
    """

    def __init__(self, storage: str):
        try:
            CertFileDBReadOnly.__init__(self, storage)
        except ValueError:
            os.makedirs(self._cert_storage, exist_ok=True)

    def insert(self, cert_id: str, cert: str) -> None:
        if not cert_id or not cert:
            raise CertInvalidError('cert_id <{}> or cert <{}> invalid'.format(cert_id, cert))
        # Save certificate to temporary file
        loc = self._create_cert_location(cert_id)
        cert_file = os.path.join(loc, make_PEM_filename(cert_id))
        if os.path.exists(cert_file):
            log.info('Certificate %s already exists', cert_file)
            return

        with open(cert_file, 'w') as w_file:
            w_file.write(cert)

        self._transaction.add(loc)
        log.debug('Certificate %s inserted to %s', cert_id, loc)

    def rollback(self) -> None:
        log.info('Rollback started')
        CertFileDB.clear_storage_block(self._transaction)
        self._transaction.clear()
        self._delete.clear()
        log.info('Rollback finished')

    def commit(self, cores=1) -> None:
        log.info('Commit started')
        # Handle delete first because sequence matter
        self._delete_certs(self._delete)
        log.info('Deleted %d certificates', len(self._delete))
        self._delete.clear()
        # Now insertion can be safely performed
        if cores > 1:
            # TODO use multiprocessing
            # import multiprocessing as mp
            for target in self._transaction:
                log.debug('Async: Persisting %s group', target)
                # add persist_and_clean_storage_dir(target in ) to pool
        else:
            for target in self._transaction:
                log.debug('Persisting %s group', target)
                CertFileDB.persist_and_clear_storage_block(target)

        self._transaction.clear()
        log.info('Commit finished')

    def delete(self, cert_id: str):
        if not cert_id:
            raise CertInvalidError('cert_id <{}> invalid'.format(cert_id))
        # Immediatelly delete certificate in open transaction if exists
        loc = self._create_cert_location(cert_id)
        cert_file = os.path.join(loc, make_PEM_filename(cert_id))
        if os.path.exists(cert_file):
            log.debug('Certificate %s deleted from open transaction', cert_id)
            os.remove(cert_file)
            if not os.listdir(loc):
                self._transaction.remove(loc)
                CertFileDB.clear_storage_block(loc)
        else:
            self._delete.add(cert_id)

    def _create_cert_location(self, cert_id: str) -> str:
        loc = self._get_cert_location(cert_id)
        # Check if location wasn't already created
        if loc not in self._transaction:
            os.makedirs(loc, exist_ok=True)

        return loc

    def _delete_certs(self, cert_id):
        # TODO group by the same block and handle the block once to improve performance
        if isinstance(cert_id, set):
            for cert in cert_id:
                self._delete_certs(cert)
        else:
            # DELETE persisted certificate from zipfile
            zipfilename = self._create_cert_location(cert_id) + '.zip'
            if not os.path.exists(zipfilename):
                return

            with ZipFile(zipfilename, 'r', ZIP_DEFLATED) as zin,\
                 ZipFile(zipfilename, 'w', ZIP_DEFLATED) as zout:
                for item in zin.infolist():
                    buffer = zin.read(item.filename)
                    if item.filename != make_PEM_filename(cert_id):
                        zout.writestr(item, buffer)
            log.debug('Certificate deleted: %s', cert_id)

    @staticmethod
    def persist_and_clear_storage_block(block):
        zipfilename = block + '.zip'
        if os.path.exists(zipfilename):
            append = True
            log.debug('Appending to zipfile: %s', zipfilename)
        else:
            append = False
            log.debug('Creating zipfile: %s', zipfilename)

        # TODO compare performance for higher compresslevel
        with ZipFile(zipfilename, "a" if append else "w", ZIP_DEFLATED) as z_obj:
            certs = os.listdir(block)
            if append:
                certs = [cert for cert in certs if cert not in z_obj.namelist()]

            for cert_name in certs:
                cert_file = os.path.join(block, cert_name)
                z_obj.write(cert_file, cert_name)

            log.debug('Persisted %d certificates', len(certs))

        CertFileDB.clear_storage_block(block)

    @staticmethod
    def clear_storage_block(block):
        if isinstance(block, set):
            for blc in block:
                CertFileDB.clear_storage_block(blc)
        else:
            shutil.rmtree(block)
            log.debug('Block %s cleared', block)

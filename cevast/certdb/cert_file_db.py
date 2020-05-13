"""
This module contains implementation of CertFileDB

    CertFileDB is a simple local database that uses files
    and a file system properties as a storage mechanism.

Storage structure on the file system:
storage                   - path to the storage given as initial parameter to CertFileDB
    - certs/
        - id[2]/         - first 2 characters of certificate ID (fingerprint) (e.g. 1a/)
            - id[4].zip  - first 4 characters of certificate ID (fingerprint) (e.g. 1a9f.zip)
            - ...
        - ...
"""

import os
import shutil
import logging
from zipfile import ZipFile, ZIP_DEFLATED
from cevast.certdb import CertDB, CertDBReadOnly, CertNotAvailableError, CertInvalidError

__author__ = 'Radim Podola'

logger = logging.getLogger(__name__)


# TODO add structure level ? higher level for more records, 0 level for common pool ?
class CertFileDBReadOnly(CertDBReadOnly):

    CERT_STORAGE_NAME = 'certs'

    def __init__(self, storage: str):
        self.storage = os.path.abspath(storage)
        self._transaction: set = set()
        self._delete: set = set()
        # Init certificate storage location
        self._cert_storage = os.path.join(self.storage, self.CERT_STORAGE_NAME)
        if not os.path.exists(self._cert_storage):
            raise ValueError('Storage location does not exists')

        logger.info(__name__ + ' initizalized...')
        logger.debug('cert storage: {}'.format(self._cert_storage))

    def get(self, id: str) -> str:
        loc = self._get_cert_location(id)
        filename = self._id_to_filename(id)
        # Check if certificate exists as a file (in case of open transaction)
        if self._transaction:
            cert_file = os.path.join(loc, filename)
            try:
                with open(cert_file, 'r') as source:
                    logger.debug('<{}> found in open transaction'.format(cert_file))
                    return source.read()
            except IOError:
                pass
        # Check if certificate exists in a zipfile
        try:
            zip_file = loc + '.zip'
            with ZipFile(zip_file, 'r', ZIP_DEFLATED) as zf:
                with zf.open(filename) as cert:
                    logger.debug('<{}> found in zip <{}>'.format(filename, zip_file))
                    return cert.read().decode('utf-8')
        except (KeyError, FileNotFoundError):
            pass

        logger.debug('<{}> not found'.format(filename))
        raise CertNotAvailableError(id)

    def export(self, id: str, target_dir: str) -> str:
        loc = self._get_cert_location(id)
        filename = self._id_to_filename(id)
        # Check if certificate exists as a file (in case of open transaction)
        if self._transaction:
            cert_src_file = os.path.join(loc, filename)
            cert_trg_file = os.path.join(target_dir, filename)
            if os.path.exists(cert_src_file):
                shutil.copyfile(cert_src_file, cert_trg_file)
                logger.debug('<{}> found in open transaction'.format(cert_src_file))
                return cert_trg_file
        # Check if certificate exists in a zipfile
        try:
            zip_file = loc + '.zip'
            with ZipFile(zip_file, 'r', ZIP_DEFLATED) as zf:
                zf.extract(filename, target_dir)
                logger.debug('<{}> found in zip <{}>'.format(filename, zip_file))
                return os.path.join(target_dir, filename)
        except (KeyError, FileNotFoundError):
            pass

        logger.debug('<{}> not found'.format(filename))
        raise CertNotAvailableError(id)

    def exists(self, id: str) -> bool:
        loc = self._get_cert_location(id)
        # Check if certificate exists as a file (in case of open transaction)
        if self._transaction:
            cert_file = os.path.join(loc, self._id_to_filename(id))
            if os.path.exists(cert_file):
                logger.debug('<{}> exists'.format(cert_file))
                return True
        # Check if certificate exists in a zipfile
        try:
            zip_file = loc + '.zip'
            with ZipFile(zip_file, 'r', ZIP_DEFLATED) as zf:
                zf.getinfo(self._id_to_filename(id))
                logger.debug('<{}> exists in <{}>'.format(id, zip_file))
                return True
        except (KeyError, FileNotFoundError):
            pass

        logger.debug('<{}> does not exist'.format(id))
        return False

    def exists_all(self, ids: list) -> bool:
        for id in ids:
            if not self.exists(id):
                return False

        return True

    def _get_cert_location(self, id: str) -> str:
        return os.path.join(self._cert_storage, id[:2], id[:4])

    # TODO move to utils
    @staticmethod
    def _id_to_filename(id: str) -> str:
        return id + '.pem'


class CertFileDB(CertDB, CertFileDBReadOnly):

    def __init__(self, storage: str):
        try:
            CertFileDBReadOnly.__init__(self, storage)
        except ValueError:
            os.makedirs(self._cert_storage, exist_ok=True)

        logger.info(__name__ + ' initizalized...')
        logger.debug('cert storage: {}'.format(self._cert_storage))

    def insert(self, id: str, cert: str) -> None:
        if not id or not cert:
            raise CertInvalidError('id <{}> or cert <{}> invalid'.format(id, cert))
        # Save certificate to temporary file
        loc = self._create_cert_location(id)
        cert_file = os.path.join(loc, self._id_to_filename(id))
        if os.path.exists(cert_file):
            logger.info('Certificate {} already exists'.format(cert_file))
            return

        with open(cert_file, 'w') as w_file:
            w_file.write(cert)

        self._transaction.add(loc)
        logger.info('Certificate {} inserted to {}'.format(cert_file, loc))

    def rollback(self) -> None:
        CertFileDB.clean_storage_dir(self._transaction)
        self._transaction.clear()
        self._delete.clear()

    def commit(self, cores=1) -> None:
        # Handle delete first because sequence matter
        self._delete_certs(self._delete)
        logger.info('Deleted {} targets'.format(len(self._delete)))
        self._delete.clear()
        # Now insertion can be safely performed
        if cores > 1:
            for target in self._transaction:
                # TODO use multiprocessing
                # import multiprocessing as mp
                for target in self._transaction:
                    logger.debug('Add target to async pool: {}'.format(target))
                    # add persist_and_clean_storage_dir(target in ) to pool
                    pass
        else:
            for target in self._transaction:
                logger.debug('Insert target: {}'.format(target))
                CertFileDB.persist_and_clean_storage_dir(target)

        logger.info('Inserted {} targets'.format(len(self._transaction)))
        self._transaction.clear()

    def delete(self, id: str):
        if not id:
            raise CertInvalidError('id <{}> invalid'.format(id))
        # Immediatelly delete certificate in open transaction if exists
        loc = self._create_cert_location(id)
        cert_file = os.path.join(loc, self._id_to_filename(id))
        if os.path.exists(cert_file):
            os.remove(cert_file)
            if not os.listdir(loc):
                self._transaction.remove(loc)
                CertFileDB.clean_storage_dir(loc)
        else:
            self._delete.add(id)

    def _create_cert_location(self, id: str) -> str:
        loc = self._get_cert_location(id)
        # Check if location wasn't already created
        if loc not in self._transaction:
            os.makedirs(loc, exist_ok=True)

        return loc

    @staticmethod
    def persist_and_clean_storage_dir(dir):
        zipfilename = dir + '.zip'
        if os.path.exists(zipfilename):
            append = True
            logger.debug('Opening zipfile: {}'.format(zipfilename))
        else:
            append = False
            logger.debug('Creating zipfile: {}'.format(zipfilename))

        # TODO compare performance for higher compresslevel
        # TODO replace ZipFile(zipfilename, "a" if append else "w", ZIP_DEFLATED) with method ?
        with ZipFile(zipfilename, "a" if append else "w", ZIP_DEFLATED) as zf:
            certs = os.listdir(dir)
            if append:
                certs = [c for c in certs if c not in zf.namelist()]

            for cert_name in certs:
                logger.debug('Zipping: {}'.format(cert_name))
                cert_file = os.path.join(dir, cert_name)
                zf.write(cert_file, cert_name)

        CertFileDB.clean_storage_dir(dir)

    @staticmethod
    def clean_storage_dir(dir):
        if type(dir) is set:
            for t in dir:
                CertFileDB.clean_storage_dir(t)
        else:
            shutil.rmtree(dir)
            logger.debug('Directory cleaned: {}'.format(dir))

    def _delete_certs(self, id):
        # TODO group by the same zipfile and handle the group once to improve performance
        if type(id) is set:
            for c in id:
                CertFileDB._delete_certs(self, c)
        else:
            # DELETE persisted certificate from zipfile
            zipfilename = self._create_cert_location(id) + '.zip'
            if not os.path.exists(zipfilename):
                return

            with ZipFile(zipfilename, 'r', ZIP_DEFLATED) as zin, \
                 ZipFile(zipfilename, 'w', ZIP_DEFLATED) as zout:
                for item in zin.infolist():
                    buffer = zin.read(item.filename)
                    if(item.filename != self._id_to_filename(id)):
                        zout.writestr(item, buffer)
            logger.debug('Certificate deleted: {}'.format(id))

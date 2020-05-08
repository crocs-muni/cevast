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
from cevast.certdb import CertDB, CertDBReadOnly, CertNotAvailableError, InvalidCertError

__author__ = 'Radim Podola'

logger = logging.getLogger(__name__)


# TODO add structure level ? higher level for more records, 0 level for common pool ?
class CertFileDBReadOnly(CertDBReadOnly):

    CERT_STORAGE_NAME = 'certs'

    def __init__(self, storage: str):
        self.storage = os.path.abspath(storage)
        self._transaction: set = set()
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
            if os.path.exists(cert_file):
                # TODO read cert
                pass
        # Check if certificate exists in a zipfile
        try:
            zip_file = loc + '.zip'
            with ZipFile(zip_file, 'r', ZIP_DEFLATED) as zf:
                with zf.open(filename) as cert:
                    return cert.read()
        except (KeyError, FileNotFoundError):
            pass

        return None

    def download(self, id: str, target_path: str) -> str:
        loc = self._get_cert_location(id)
        filename = self._id_to_filename(id)
        # Check if certificate exists as a file (in case of open transaction)
        if self._transaction:
            cert_src_file = os.path.join(loc, filename)
            cert_trg_file = os.path.join(target_path, filename)
            if os.path.exists(cert_src_file):
                with open(cert_src_file, 'r') as source, \
                     open(cert_trg_file, "w") as target:
                    shutil.copyfileobj(source, target)
        # Check if certificate exists in a zipfile
        try:
            zip_file = loc + '.zip'
            with ZipFile(zip_file, 'r', ZIP_DEFLATED) as zf:
                zf.extract(filename, target_path)
                return os.path.join(target_path, filename)
        except (KeyError, FileNotFoundError):
            pass

        return None

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

    # TODO some parameter validation ??
    def insert(self, id: str, cert: str):
        loc = self._create_cert_location(id)
        cert_file = os.path.join(loc, self._id_to_filename(id))

        # TODO move next line to the parser
        content = '-----BEGIN CERTIFICATE-----' + '\n' + cert + '\n' + '-----END CERTIFICATE-----'
        with open(cert_file, 'w') as w_file:
            w_file.write(content)

        self._transaction.add(loc)

        logger.info('Certificate {} inserted to {}'.format(cert_file, loc))

    def rollback(self):
        CertFileDB.clean_storage_target(self._transaction)
        self._transaction.clear()

    def commit(self, cores=1):
        if cores > 1:
            for target in self._transaction:
                # TODO use multiprocessing
                # import multiprocessing as mp
                for target in self._transaction:
                    logger.debug('Add target to async pool: {}'.format(target))
                    # add persist_and_clean_storage_target(target in ) to pool
                    pass
        else:
            for target in self._transaction:
                logger.debug('Commit target: {}'.format(target))
                CertFileDB.persist_and_clean_storage_target(target)

        logger.info('Committed {} targets'.format(len(self._transaction)))
        self._transaction.clear()

    def _create_cert_location(self, id: str) -> str:
        loc = self._get_cert_location(id)
        # Check if location wasn't already created
        if loc not in self._transaction:
            os.makedirs(loc, exist_ok=True)

        return loc

    @staticmethod
    def persist_and_clean_storage_target(target):
        zipfilename = target + '.zip'
        if os.path.exists(zipfilename):
            append = True
            logger.debug('Opening zipfile: {}'.format(zipfilename))
        else:
            append = False
            logger.debug('Creating zipfile: {}'.format(zipfilename))

        with ZipFile(zipfilename, "a" if append else "w", ZIP_DEFLATED) as zf:
            certs = os.listdir(target)
            if append:
                certs = [c for c in certs if c not in zf.namelist()]

            for cert_name in certs:
                logger.debug('Zipping: {}'.format(cert_name))
                cert_file = os.path.join(target, cert_name)
                zf.write(cert_file, cert_name)

        CertFileDB.clean_storage_target(target)

    @staticmethod
    def clean_storage_target(target):
        if type(target) is set:
            for t in target:
                CertFileDB.clean_storage_target(t)
        else:
            shutil.rmtree(target)

        logger.debug('Target cleaned: {}'.format(target))

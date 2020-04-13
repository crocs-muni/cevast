"""
This module contains components of CertFileDB

    CertFileDB is a simple local database that uses files
    and a file system properties as a storage mechanism.

Storage structure on the file system:
storage                  - path given as initial parameter to CertFileDB
    - dataset_certs/
        - [date]/        - date folder groups server certificates from dataset (e.g. 2019-12-30)
            - [sha].zip  - first 2 characters of SHA fingerprint (e.g. 1a.zip)
        - ...
        - shared/        - non-server certificates from all managed datasets
            - [sha].zip  - first 2 characters of SHA fingerprint (e.g. 99.zip)
    - common_pool/       - additional pool of common certificates downloaded separately from datasets
"""

import os
import shutil
import logging
import zipfile

__author__ = 'Radim Podola'

logger = logging.getLogger(__name__)

DATASET_CERT_STORAGE_NAME = 'dataset_certs'
DATASET_CERT_SHARED_STORAGE_NAME = 'shared'
COMMON_POOL_STORAGE_NAME = 'common_pool'


class CertNotAvailableError(Exception):
    """Raised when the certificate is not available in database"""
    pass


class CertFileDB:

    def __init__(self, storage: str, date: str):
        self.storage = os.path.abspath(storage)
        self.date = date
        self.__waiting_for_commit: list = []
        self.__init_storage()

        logger.info(__name__ + ' initizalized...')
        logger.debug('storage: {}'.format(self.storage))
        logger.debug('date: {}'.format(self.date))

    def __init_storage(self):
        self.__dcert_storage = os.path.join(self.storage, DATASET_CERT_STORAGE_NAME)
        self.__dcert_date_storage = os.path.join(self.__dcert_storage, self.date)
        self.__dcert_shared_storage = os.path.join(self.__dcert_storage, DATASET_CERT_SHARED_STORAGE_NAME)
        os.makedirs(self.__dcert_date_storage, exist_ok=True)
        os.makedirs(self.__dcert_shared_storage, exist_ok=True)

    def get(self, target_folder):
        pass

    def exists(self, sha: str, server_cert: bool = True):
        trg_path = self.__create_cert_path_str(sha, server_cert)

        # TODO add also exists in zip files

        return os.path.exists(os.path.join(trg_path, sha + '.pem'))

    def insert(self, sha: str, cert: str, server_cert: bool = True):
        trg_path = self.__create_cert_path(sha, server_cert)
        filename = os.path.join(trg_path, sha + '.pem')

        content = '-----BEGIN CERTIFICATE-----' + '\n' + cert + '\n' + '-----END CERTIFICATE-----'
        with open(filename, 'w') as w_file:
            w_file.write(content)

        self.__waiting_for_commit.append(trg_path)

        logger.info('Certificate {} inserted to {}'.format(filename, trg_path))

    def rollback(self):
        CertFileDB.clean_storage_target(self.__waiting_for_commit)
        self.__waiting_for_commit.clear()

    def commit(self, cores=1):
        for target in self.__waiting_for_commit:
            if cores > 1:
                # TODO use multiprocessing
                # import multiprocessing as mp
                for target in self.__waiting_for_commit:
                    logger.debug('Add target to async pool: {}'.format(target))
                    # add persist_and_clean_storage_target(target in ) to pool
                    pass
            else:
                logger.debug('Commit target: {}'.format(target))
                CertFileDB.persist_and_clean_storage_target(target)

        logger.info('Committed {} targets'.format(len(self.__waiting_for_commit)))
        self.__waiting_for_commit.clear()

    def __create_cert_path_str(self, sha: str, server_cert: bool = True) -> str:
        if server_cert:
            trg_path = os.path.join(self.__dcert_date_storage, sha[:2])
        else:
            trg_path = os.path.join(self.__dcert_shared_storage, sha[:2])

        return trg_path

    def __create_cert_path(self, sha: str, server_cert: bool = True) -> str:
        trg_path = self.__create_cert_path_str(sha, server_cert)
        os.makedirs(trg_path, exist_ok=True)

        return trg_path

    @staticmethod
    def persist_and_clean_storage_target(target):
        zipfilename = target + '.zip'
        if os.path.exists(zipfilename):
            append = True
            logger.debug('Opening zipfile: {}'.format(zipfilename))
        else:
            append = False
            logger.debug('Creating zipfile: {}'.format(zipfilename))

        with zipfile.ZipFile(zipfilename, "a" if append else "w", zipfile.ZIP_DEFLATED) as zf:
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
        if type(target) is list:
            for t in target:
                CertFileDB.clean_storage_target(t)
        else:
            shutil.rmtree(target)

        logger.debug('Target cleaned: {}'.format(target))

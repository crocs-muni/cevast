"""
This module contains implementation of CertFileDB

    CertFileDB is a simple local database that uses files
    and a file system properties as a storage mechanism.

Storage structure on the file system:
storage/             - path to the storage given as an initial parameter to CertFileDB containing
                       hierarchy of certificate blocks (group of certificates with equal prefix)
    - id[2]/         - first 2 characters of certificate ID (fingerprint) make block (e.g. 1a/)
        - id[3].zip  - first 2 characters of certificate ID (fingerprint) (e.g. 1af.zip)
        - ...
    - ...
    - .CertFileDB.toml    - CertFileDB configuration file

.CertFileDB.toml example:
[PARAMETERS]
storage = "/var/tmp/cevast_storage"
structure_level = 2
cert_format = "PEM"
compression_method = "ZIP_DEFLATED"

[INFO]
owner = "cevast"
description = "Certificate storage for Cevast tool"
created = "2020-02-30 14:23:18"
number_of_certificates = 2013562
last_commit = "2020-05-30 22:44:48"

[HISTORY]
a = 1
b = 2
"2020-05-30 22:44:48" = "added=5; removed=1;"
"""

import os
import shutil
import logging
from typing import Tuple
from datetime import datetime
from collections import OrderedDict
from zipfile import ZipFile, ZIP_DEFLATED
import toml
from cevast.utils import make_PEM_filename, remove_empty_folders
from cevast.certdb.cert_db import (
    CertDB,
    CertDBReadOnly,
    CertNotAvailableError,
    CertInvalidError,
)

__author__ = 'Radim Podola'

log = logging.getLogger(__name__)

# TODO parallel transaction checking - mmap
# - open transaction Flag - will be set by INSERT/REMOVE/ROLLBACK/COMMIT -> OpenTransaction/CloseTransaction decorator ??
# - allow_more_transaction Flag that will not raise DBInUse error??
# TODO maintain history upon commits
# TODO make persist_and_clear_storage/clear_storage utility method that will not use transaction data


class CertFileDBReadOnly(CertDBReadOnly):
    """
    CertDBReadOnly interface implementation which uses files
    and a file system properties as a storage mechanism.
    """

    CONF_FILENAME = '.CertFileDB.toml'

    @staticmethod
    def setup(storage_path: str, structure_level: int = 2, cert_format: str = 'PEM',
                desc: str = 'CertFileDB', owner: str = '') -> None:
        """
        Setup CertFileDB storage directory with the given parameters.
        Directory and configuration file CertFileDB.toml is created.
        Raise ValueError for wrong parameters or if DB already exists.
        """
        storage_path = os.path.abspath(storage_path)
        config_path = os.path.join(storage_path, CertFileDB.CONF_FILENAME)
        if os.path.exists(config_path):
            raise ValueError('CertFileDB already exists')
        if not isinstance(structure_level, int):
            raise ValueError('structure_level must be an integer')
        os.makedirs(storage_path, exist_ok=True)
        # Create configuration file
        config = OrderedDict()
        config['PARAMETERS'] = OrderedDict()
        config['PARAMETERS']['storage'] = storage_path
        config['PARAMETERS']['structure_level'] = structure_level
        config['PARAMETERS']['cert_format'] = cert_format
        config['PARAMETERS']['compression_method'] = 'ZIP_DEFLATED'
        config['INFO'] = OrderedDict()
        config['INFO']['owner'] = owner
        config['INFO']['description'] = desc
        config['INFO']['created'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S%Z')
        config['INFO']['number_of_certificates'] = 0
        config['INFO']['last_commit'] = ''
        config['HISTORY'] = OrderedDict()
        with open(config_path, 'w') as cfg_file:
            toml.dump(config, cfg_file)

    def __init__(self, storage: str):
        # Get config
        try:
            config_path = os.path.join(os.path.abspath(storage), self.CONF_FILENAME)
            self.storage = os.path.abspath(storage)
            config = toml.load(config_path)
            self._params = config['PARAMETERS']
            log.info('Found CertFileDB <%s>:\n%s', config_path, config)
        except FileNotFoundError:
            raise ValueError('CertFileDB <{}> does not exists -> call CertFileDB.setup() first'.format(config_path))
        # Init DB instance
        log.info('Initializing %s...', self.__class__.__name__)
        # Set maintaining all known certificate IDs for better EXISTS performance
        self._cache: set = set()

    def get(self, cert_id: str) -> str:
        block = self._get_block_path(cert_id)
        filename = make_PEM_filename(cert_id)
        # Check if certificate exists
        try:
            zip_file = block + '.zip'
            with ZipFile(zip_file, 'r', ZIP_DEFLATED) as z_obj:
                with z_obj.open(filename) as cert:
                    log.debug('<%s> found persisted in zip <%s>', filename, zip_file)
                    return cert.read().decode('utf-8')
        except (KeyError, FileNotFoundError):
            pass

        log.info('<%s> not found', filename)
        raise CertNotAvailableError(cert_id)

    def export(self, cert_id: str, target_dir: str, copy_if_exists: bool = True) -> str:
        block = self._get_block_path(cert_id)
        filename = make_PEM_filename(cert_id)
        # Check if certificate exists persisted
        try:
            zip_file = block + '.zip'
            with ZipFile(zip_file, 'r', ZIP_DEFLATED) as z_obj:
                z_obj.extract(filename, target_dir)
                log.debug('<%s> found persisted in zip <%s>', filename, zip_file)
                return os.path.join(target_dir, filename)
        except (KeyError, FileNotFoundError):
            pass

        log.info('<%s> not found', filename)
        raise CertNotAvailableError(cert_id)

    def exists(self, cert_id: str) -> bool:
        # Check cache first
        if cert_id in self._cache:
            log.debug('<%s> found in cache', cert_id)
            return True

        block = self._get_block_path(cert_id)
        cert_filename = make_PEM_filename(cert_id)
        # Check if certificate exists persisted
        try:
            zip_file = block + '.zip'
            with ZipFile(zip_file, 'r', ZIP_DEFLATED) as z_obj:
                z_obj.getinfo(cert_filename)
                log.debug('<%s> exists persisted <%s>', cert_id, zip_file)
                self._cache.add(cert_id)
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

    def _get_block_path(self, cert_or_block_id: str) -> str:
        """Return full block path of certificate or block id"""
        paths = [cert_or_block_id[: 2 + i] for i in range(self._params['structure_level'])]
        return os.path.join(self._params['storage'], *paths)


class CertFileDB(CertDB, CertFileDBReadOnly):
    """
    CertDB interface implementation which uses files
    and a file system properties as a storage mechanism.
    """

    def __init__(self, storage: str):
        CertFileDBReadOnly.__init__(self, storage)
        # Dict containing all inserted certificates grouped in blocks that will be persisted with commit
        self._to_insert: dict = {}
        # Dict containing all deleted certificates grouped in blocks that will be deleted with commit
        self._to_delete: dict = {}

    def get(self, cert_id: str) -> str:
        block = self._get_block_path(cert_id)
        filename = make_PEM_filename(cert_id)
        # Check if certificate exists as a file (transaction still open)
        if self._is_in_transaction(cert_id, self._to_insert):
            cert_file = os.path.join(block, filename)
            with open(cert_file, 'r') as source:
                log.debug('<%s> found in open transaction', cert_file)
                return source.read()
        # Check if certificate exists persisted
        return CertFileDBReadOnly.get(self, cert_id)

    def export(self, cert_id: str, target_dir: str, copy_if_exists: bool = True) -> str:
        block = self._get_block_path(cert_id)
        filename = make_PEM_filename(cert_id)
        # Check if certificate exists as a file (transaction still open)
        if self._is_in_transaction(cert_id, self._to_insert):
            cert_src_file = os.path.join(block, filename)
            log.debug('<%s> found in open transaction', cert_src_file)
            if not copy_if_exists:
                return cert_src_file
            # Copy file to the target directory
            cert_trg_file = os.path.join(target_dir, filename)
            shutil.copyfile(cert_src_file, cert_trg_file)
            return cert_trg_file
        # Check if certificate exists persisted
        return CertFileDBReadOnly.export(self, cert_id, target_dir, copy_if_exists)

    def exists(self, cert_id: str) -> bool:
        # Check the open transaction first
        if self._is_in_transaction(cert_id, self._to_insert):
            log.debug('<%s> exists in open transaction', cert_id)
            return True
        # Check if certificate exists persisted
        return CertFileDBReadOnly.exists(self, cert_id)

    def insert(self, cert_id: str, cert: str) -> None:
        if not cert_id or not cert:
            raise CertInvalidError('cert_id <{}> or cert <{}> invalid'.format(cert_id, cert))
        # Save certificate to temporary file
        block = self._get_block_path(cert_id)
        if self._get_block_id(cert_id) not in self._to_insert:
            os.makedirs(block, exist_ok=True)
        cert_file = os.path.join(block, make_PEM_filename(cert_id))
        if os.path.exists(cert_file):
            log.info('Certificate %s already exists', cert_file)
            return

        with open(cert_file, 'w') as w_file:
            w_file.write(cert)
        # Add certificate to transaction for insert upon commit
        self._add_to_transaction(cert_id, self._to_insert)
        log.debug('Certificate %s inserted to block %s', cert_id, block)

    def delete(self, cert_id: str):
        if not cert_id:
            raise CertInvalidError('cert_id <{}> invalid'.format(cert_id))

        if self._is_in_transaction(cert_id, self._to_insert):
            # Immediatelly delete certificate in open transaction if exists
            block = self._get_block_path(cert_id)
            cert_file = os.path.join(block, make_PEM_filename(cert_id))
            self._remove_from_transaction(cert_id, self._to_insert)
            os.remove(cert_file)
            log.debug('Certificate %s deleted from open transaction', cert_id)
        else:
            # Add certificate to transaction for delete upon commit
            self._add_to_transaction(cert_id, self._to_delete)
            log.debug('Certificate %s will be deleted upon commit', cert_id)

    def rollback(self) -> None:
        log.info('Rollback started')
        # Remove uncommitted certificates
        for block, certs in self._to_insert.items():
            block_path = self._get_block_path(block)
            for cert in certs:
                os.remove(os.path.join(block_path, make_PEM_filename(cert)))
        self._to_insert.clear()
        self._to_delete.clear()
        # Clean up empty folders
        remove_empty_folders(self.storage)
        log.info('Rollback finished')

    def commit(self, cores=1) -> Tuple[int, int]:
        log.info('Commit started')
        cnt_deleted = 0
        cnt_inserted = 0
        # Handle delete first because sequence matter
        # TODO use multiprocessing
        for block, certs in self._to_delete:
            cnt_deleted += CertFileDB.delete_certs(self._get_block_path(block), certs)
            # Delete certificates also from cache
            self._cache -= certs

        self._to_delete.clear()
        log.info('Deleted %d certificates', cnt_deleted)
        # Now handle insert
        # TODO use multiprocessing
        for block, certs in self._to_insert.items():
            cnt_inserted += CertFileDB.persist_certs(self._get_block_path(block), certs)

        self._to_insert.clear()
        log.info('Inserted %d certificates', cnt_inserted)
        # Clean up empty folders
        remove_empty_folders(self.storage)
        log.info('Commit finished')
        return cnt_inserted, cnt_deleted

    # static so I can use it in async pool or find a way hot to use private
    @staticmethod
    def delete_certs(block: str, certs: set) -> int:
        """Delete persisted certificates from block"""
        cnt_deleted = 0
        zipfilename = block + '.zip'
        if certs and os.path.exists(zipfilename):
            deleted_all = True
            new_zipfilename = zipfilename + '_new'
            with ZipFile(zipfilename, 'r', ZIP_DEFLATED) as zin,\
                 ZipFile(new_zipfilename, 'w', ZIP_DEFLATED) as zout:
                for name in zin.namelist():
                    if os.path.splitext(name)[0] not in certs:
                        zout.writestr(name, zin.read(name))
                        deleted_all = False
                    else:
                        cnt_deleted += 1
            # Remove the original zipfile and replace it with new one
            os.remove(zipfilename)
            if deleted_all:
                # Delete the empty zipfile
                os.remove(new_zipfilename)
            else:
                os.rename(new_zipfilename, zipfilename)

        log.debug('Deleted %d certificates from block %s', cnt_deleted, block)
        return cnt_deleted

    # static so I can use it in async pool
    @staticmethod
    def persist_certs(block: str, certs: set) -> int:
        """Persist certificates to block"""
        if not certs:
            log.debug('Nothing to insert in block %s', block)
            return 0
        cnt_inserted = 0
        zipfilename = block + '.zip'
        if os.path.exists(zipfilename):
            append = True
            log.debug('Appending to zipfile: %s', zipfilename)
        else:
            append = False
            log.debug('Creating zipfile: %s', zipfilename)

        # TODO compare performance for higher compresslevel
        with ZipFile(zipfilename, "a" if append else "w", ZIP_DEFLATED) as zout:
            if append:
                persisted_certs = zout.namelist()

            for cert in certs:
                cert_name = make_PEM_filename(cert)
                cert_file = os.path.join(block, cert_name)
                if append and cert_name in persisted_certs:
                    pass
                else:
                    zout.write(cert_file, cert_name)
                    cnt_inserted += 1
                os.remove(cert_file)

        log.debug('Persisted %d certificates from block %s', cnt_inserted, block)
        return cnt_inserted

    def _is_in_transaction(self, cert_id: str, trans_dict: dict) -> bool:
        return cert_id in trans_dict.get(self._get_block_id(cert_id), {})

    def _add_to_transaction(self, cert_id: str, trans_dict: dict) -> None:
        block_id = self._get_block_id(cert_id)
        if block_id not in trans_dict:
            trans_dict[block_id] = set()
        trans_dict[block_id].add(cert_id)

    def _remove_from_transaction(self, cert_id: str, trans_dict: dict) -> None:
        trans_dict[self._get_block_id(cert_id)].discard(cert_id)

    def _get_block_id(self, cert_id: str) -> str:
        return cert_id[: self._params['structure_level'] + 1]

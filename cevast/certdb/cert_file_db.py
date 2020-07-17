"""
This module contains implementation of CertFileDB

    CertFileDB is a simple local database implementing CertDB interface that
    uses files and a file system properties as a storage mechanism.

.. todo::Put this in separate Markdown

Storage structure on the file system:
storage/             - path to the storage given as an initial parameter to CertFileDB containing
                       hierarchy of certificate blocks (group of certificates with equal prefix)
    - id[2]/         - first 2 characters of certificate ID (fingerprint) make block (e.g. 1a/)
        - id[3].zip  - first 2 characters of certificate ID (fingerprint) (e.g. 1af.zip)
        - ...
    - ...
    - .CertFileDB.toml       - CertFileDB configuration file
    - .CertFileDB-META.toml  - CertFileDB meta-information file

.CertFileDB.toml example:
[PARAMETERS]
storage = "/var/tmp/cevast_storage"
structure_level = 2
cert_format = "PEM"
compression_method = "ZIP_DEFLATED"
maintain_info = True

.CertFileDB-META.toml example:
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
import multiprocessing as mp
from typing import Tuple
from datetime import datetime
from collections import OrderedDict
from zipfile import ZipFile, ZIP_DEFLATED, ZIP_STORED
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
# - or reserve block?? mmap vector of flags (up to 256 els) - each element is root block
# TODO make persist_and_clear_storage/clear_storage utility method that will not use transaction data
# TODO check this out https://github.com/ThomasPinna/python_zipfile_improvement/tree/master


class CertFileDBReadOnly(CertDBReadOnly):
    """
    CertDBReadOnly interface implementation which uses files
    and a file system properties as a storage mechanism.

    `storage` is path to the database storage.
    """

    CONF_FILENAME = 'CertFileDB.toml'
    META_FILENAME = '.CertFileDB-META.toml'

    @staticmethod
    def setup(storage_path: str, structure_level: int = 2, cert_format: str = 'PEM',
              desc: str = 'CertFileDB', owner: str = '', maintain_info: bool = True) -> None:
        """
        Setup CertFileDB storage directory with the given parameters.
        `storage_path` is path to the database root directory,
        `structure_level` is hierarchy level of certificate blocks,
        `cert_format` is used format of stored certificates,
        `desc` is database description,
        `owner` is database owner,
        `maintain_info` is flag whether to maintain META file or not.

        Directory, configuration and META file is created.
        Raise ValueError for wrong parameters or if DB already exists.
        """
        storage_path = os.path.abspath(storage_path)
        config_path = os.path.join(storage_path, CertFileDB.CONF_FILENAME)
        meta_path = os.path.join(storage_path, CertFileDB.META_FILENAME)
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
        config['PARAMETERS']['compression_method'] = 'ZIP_STORED'
        config['PARAMETERS']['maintain_info'] = maintain_info
        with open(config_path, 'w') as cfg_file:
            toml.dump(config, cfg_file)
        # Create META file
        meta = OrderedDict()
        meta['INFO'] = OrderedDict()
        meta['INFO']['owner'] = owner
        meta['INFO']['description'] = desc
        meta['INFO']['created'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S%Z')
        with open(meta_path, 'w') as meta_file:
            toml.dump(meta, meta_file)

        log.info('CertFileDB was setup:\n%s', config)

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
        log.info('Initializing %s transaction...', self.__class__.__name__)
        # Set maintaining all known certificate IDs for better EXISTS performance
        self._cache: set = set()
        # Pre-compute index used for block_id
        self._block_id_index = self._params['structure_level'] + 1

    def get(self, cert_id: str) -> str:
        # Check if certificate exists
        try:
            zip_file = self._get_block_archive(cert_id)
            with ZipFile(zip_file, 'r', ZIP_STORED) as z_obj:
                with z_obj.open(cert_id) as cert:
                    log.debug('<%s> found persisted in zip <%s>', cert_id, zip_file)
                    return cert.read().decode('utf-8')
        except (KeyError, FileNotFoundError):
            pass

        log.info('<%s> not found', cert_id)
        if cert_id in self._cache:
            self._cache.clear()  # Cache seems to be invalidated, so clear it
        raise CertNotAvailableError(cert_id)

    def export(self, cert_id: str, target_dir: str, copy_if_exists: bool = True) -> str:
        # Check if certificate exists persisted
        try:
            zip_file = self._get_block_archive(cert_id)
            with ZipFile(zip_file, 'r', ZIP_STORED) as z_obj:
                zipinfo = z_obj.getinfo(cert_id)
                zipinfo.filename = make_PEM_filename(cert_id)
                z_obj.extract(zipinfo, target_dir)
                log.debug('<%s> found persisted in zip <%s>', cert_id, zip_file)
                return os.path.join(target_dir, make_PEM_filename(cert_id))
        except (KeyError, FileNotFoundError):
            pass

        log.info('<%s> not found', cert_id)
        if cert_id in self._cache:
            self._cache.clear()  # Cache seems to be invalidated, so clear it
        raise CertNotAvailableError(cert_id)

    def exists(self, cert_id: str) -> bool:
        # Check cache first
        if cert_id in self._cache:
            log.debug('<%s> found in cache', cert_id)
            return True

        # Check if certificate exists persisted
        try:
            zip_file = self._get_block_archive(cert_id)
            with ZipFile(zip_file, 'r', ZIP_STORED) as z_obj:
                z_obj.getinfo(cert_id)
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
        return "/".join([self._params['storage']] + paths) + '/'

    def _get_block_id(self, cert_id: str) -> str:  # pylint: disable=E0202
        return cert_id[: self._block_id_index]

    def _get_block_archive(self, cert_or_block_id: str) -> str:
        block_path = self._get_block_path(cert_or_block_id)
        return block_path + self._get_block_id(cert_or_block_id) + '.zip'


class CertFileDB(CertDB, CertFileDBReadOnly):
    """
    CertDB interface implementation which uses files
    and a file system properties as a storage mechanism.

    `storage` is path to the database storage.
    `cpu_cores` is max number of CPU cores that might be used.
    """

    def __init__(self, storage: str, cpu_cores: int = 1):
        CertFileDBReadOnly.__init__(self, storage)
        # Dict containing all inserted certificates grouped in blocks that will be persisted with commit
        self._to_insert: dict = {}
        # Dict containing all deleted certificates grouped in blocks that will be deleted with commit
        self._to_delete: dict = {}
        # Max number of CPU cores that can be used (-1 is max limit by hardware)
        self.__cpu_cores = int(cpu_cores)
        log.info('Will use %d CPUs', self.__cpu_cores)
        # Redefine _get_block_id method for special case with structure_level = 0
        if self._params['structure_level'] == 0:
            fixed_block_id = os.path.basename(storage)
            self._get_block_id = lambda _: fixed_block_id

    def get(self, cert_id: str) -> str:
        # Check if certificate exists as a file (transaction still open)
        if self._is_in_transaction(cert_id, self._to_insert):
            cert_file = self._get_block_path(cert_id) + cert_id
            with open(cert_file, 'r') as source:
                log.debug('<%s> found in open transaction', cert_file)
                return source.read()
        # Check if certificate is scheduled for delete
        if self._is_in_transaction(cert_id, self._to_delete):
            log.info('<%s> was deleted in current transaction', cert_id)
            raise CertNotAvailableError(cert_id)
        # Check if certificate exists persisted
        return CertFileDBReadOnly.get(self, cert_id)

    def export(self, cert_id: str, target_dir: str, copy_if_exists: bool = True) -> str:
        # Check if certificate exists as a file (transaction still open)
        if self._is_in_transaction(cert_id, self._to_insert):
            cert_src_file = self._get_block_path(cert_id) + cert_id
            log.debug('<%s> found in open transaction', cert_src_file)
            if not copy_if_exists:
                return cert_src_file
            # Copy file to the target directory
            cert_trg_file = os.path.join(target_dir, make_PEM_filename(cert_id))
            shutil.copyfile(cert_src_file, cert_trg_file)
            return cert_trg_file
        # Check if certificate is scheduled for delete
        if self._is_in_transaction(cert_id, self._to_delete):
            log.info('<%s> was deleted in current transaction', cert_id)
            raise CertNotAvailableError(cert_id)
        # Check if certificate exists persisted
        return CertFileDBReadOnly.export(self, cert_id, target_dir, copy_if_exists)

    def exists(self, cert_id: str) -> bool:
        # Check the open transaction first
        if self._is_in_transaction(cert_id, self._to_insert):
            log.debug('<%s> exists in open transaction', cert_id)
            return True
        # Check if certificate is scheduled for delete
        if self._is_in_transaction(cert_id, self._to_delete):
            log.info('<%s> was deleted in current transaction', cert_id)
            return False
        # Check if certificate exists persisted
        return CertFileDBReadOnly.exists(self, cert_id)

    def insert(self, cert_id: str, cert: str) -> None:
        if not cert_id or not cert:
            raise CertInvalidError('cert_id <{}> or cert <{}> invalid'.format(cert_id, cert))
        # Save certificate to temporary file
        block = self._get_block_path(cert_id)
        cert_file = block + cert_id
        if os.path.exists(cert_file):
            log.info('Certificate %s already exists', cert_file)
        else:
            try:
                with open(cert_file, 'w') as w_file:
                    w_file.write(cert)
            except FileNotFoundError:
                os.makedirs(block, exist_ok=True)
                with open(cert_file, 'w') as w_file:
                    w_file.write(cert)
        # Add certificate to transaction for insert upon commit
        self._add_to_transaction(cert_id, self._to_insert)
        log.debug('Certificate %s inserted to block %s', cert_id, block)

    def delete(self, cert_id: str) -> None:
        if not cert_id:
            raise CertInvalidError('cert_id <{}> invalid'.format(cert_id))

        if self._is_in_transaction(cert_id, self._to_insert):
            # Immediatelly delete certificate in open transaction if exists
            cert_file = self._get_block_path(cert_id) + cert_id
            self._remove_from_transaction(cert_id, self._to_insert)
            os.remove(cert_file)
            log.debug('Certificate %s deleted from open transaction', cert_id)
        else:
            # Add certificate to transaction for delete upon commit
            self._add_to_transaction(cert_id, self._to_delete)
            log.debug('Certificate %s will be deleted upon commit', cert_id)

        # Delete certificates from cache
        self._cache.discard(cert_id)

    def rollback(self) -> None:
        log.info('Rollback started')
        # Remove uncommitted certificates
        for block, certs in self._to_insert.items():
            block_path = self._get_block_path(block)
            for cert_id in certs:
                os.remove(block_path + cert_id)
        self._to_insert.clear()
        self._to_delete.clear()
        # Clean up empty folders
        remove_empty_folders(self.storage)
        log.info('Rollback finished')

    def commit(self) -> Tuple[int, int]:
        log.info('Commit started')
        cnt_deleted = 0
        cnt_inserted = 0

        if self.__cpu_cores != 1:
            cnt_inserted, cnt_deleted = self.__commit_async()
        else:
            # Handle delete first because sequence matter
            for block, certs in self._to_delete.items():
                cnt_deleted += CertFileDB.delete_certs(self._get_block_archive(block), certs)
            # Now handle insert
            for block, certs in self._to_insert.items():
                cnt_inserted += CertFileDB.persist_certs(self._get_block_path(block), self._get_block_archive(block), certs)

        self._to_delete.clear()
        self._to_insert.clear()
        log.info('Deleted %d certificates', cnt_deleted)
        log.info('Inserted %d certificates', cnt_inserted)
        # Clean up empty folders
        remove_empty_folders(self.storage)  # TODO seems not working properly in benchmark
        # Write commit info
        if self._params['maintain_info']:
            self.__write_commit_info(cnt_inserted, cnt_deleted)
        log.info('Commit finished')
        return cnt_inserted, cnt_deleted

    def __commit_async(self) -> Tuple[int, int]:
        """
        Function acomplishing the same as commit() but with use of multiprocessing.Pool
        of asynchronous workers to persist/delete multiple certificate blocks in parallel.
        """
        cnt_deleted = 0
        cnt_inserted = 0
        cpus = self.__cpu_cores if self.__cpu_cores > 0 else None

        pool = mp.Pool(cpus)
        # Handle delete first because sequence matter
        results = []
        for block, certs in self._to_delete.items():
            results.append(pool.apply_async(CertFileDB.delete_certs, args=(self._get_block_archive(block), certs)))
        cnt_deleted = sum([result.get() for result in results])
        # Now handle insert
        results = []
        for block, certs in self._to_insert.items():
            results.append(
                pool.apply_async(
                    CertFileDB.persist_certs, args=(self._get_block_path(block), self._get_block_archive(block), certs)
                )
            )
        cnt_inserted = sum([result.get() for result in results])
        pool.close()
        pool.join()

        return cnt_inserted, cnt_deleted

    # static so I can use it in async pool or find a way hot to use private
    @staticmethod
    def delete_certs(block_archive: str, certs: set) -> int:
        """Delete persisted certificates from block archive"""
        cnt_deleted = 0
        if certs and os.path.exists(block_archive):
            deleted_all = True
            new_block_archive = block_archive + '_new'
            with ZipFile(block_archive, 'r', ZIP_STORED) as zin,\
                 ZipFile(new_block_archive, 'w', ZIP_STORED) as zout:
                for name in zin.namelist():
                    if os.path.splitext(name)[0] not in certs:
                        zout.writestr(name, zin.read(name))
                        deleted_all = False
                    else:
                        cnt_deleted += 1
            # Remove the original zipfile and replace it with new one
            os.remove(block_archive)
            if deleted_all:
                # Delete the empty zipfile
                os.remove(new_block_archive)
            else:
                os.rename(new_block_archive, block_archive)

        log.debug('Deleted %d certificates from block %s', cnt_deleted, block_archive)
        return cnt_deleted

    # static so I can use it in async pool
    @staticmethod
    def persist_certs(block_path: str, block_archive: str, certs: set) -> int:
        """Persist certificates to block archive"""
        if not certs:
            log.debug('Nothing to insert from block %s', block_path)
            return 0
        cnt_inserted = 0
        if os.path.exists(block_archive):
            append = True
            log.debug('Appending to archive: %s', block_archive)
        else:
            append = False
            log.debug('Creating archive: %s', block_archive)

        # TODO compare performance for higher compresslevel
        with ZipFile(block_archive, "a" if append else "w", ZIP_STORED) as zout:
            if append:
                persisted_certs = zout.namelist()

            for cert in certs:
                cert_file = block_path + cert
                if append and cert in persisted_certs:
                    pass  # do not insert duplicates
                else:
                    zout.write(cert_file, cert)
                    cnt_inserted += 1
                os.remove(cert_file)

        log.debug('Persisted %d certificates from block %s', cnt_inserted, block_path)
        return cnt_inserted

    def _is_in_transaction(self, cert_id: str, trans_dict: dict) -> bool:
        return cert_id in trans_dict.get(self._get_block_id(cert_id), {})

    def _add_to_transaction(self, cert_id: str, trans_dict: dict) -> None:
        block_id = self._get_block_id(cert_id)
        try:
            trans_dict[block_id].add(cert_id)
        except KeyError:
            trans_dict[block_id] = set()
            trans_dict[block_id].add(cert_id)

    def _remove_from_transaction(self, cert_id: str, trans_dict: dict) -> None:
        block_id = self._get_block_id(cert_id)
        try:
            trans_dict[block_id].discard(cert_id)
            if not trans_dict[block_id]:
                del trans_dict[block_id]
        except KeyError:
            pass

    def __write_commit_info(self, inserted: int, deleted: int) -> None:
        meta_path = os.path.join(self.storage, self.META_FILENAME)
        if not os.path.exists(meta_path):
            # Create META file
            meta = OrderedDict()
            meta['INFO'] = OrderedDict()
            meta['INFO']['owner'] = ""
            meta['INFO']['description'] = ""
            meta['INFO']['created'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S%Z')
        else:
            meta = toml.load(meta_path, OrderedDict)
        # Update DB INFO
        total_cnt = meta['INFO'].get('number_of_certificates', 0)
        meta['INFO']['number_of_certificates'] = total_cnt + inserted - deleted
        meta['INFO']['last_commit'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S%Z')
        # Append commit HISTORY
        if 'HISTORY' not in meta:
            meta['HISTORY'] = OrderedDict()
        commit_nr = str(len(meta['HISTORY']) + 1)
        meta['HISTORY'][commit_nr] = OrderedDict()
        meta['HISTORY'][commit_nr]['date'] = meta['INFO']['last_commit']
        meta['HISTORY'][commit_nr]['inserted'] = inserted
        meta['HISTORY'][commit_nr]['deleted'] = deleted

        log.debug(meta)
        with open(meta_path, 'w') as meta_file:
            toml.dump(meta, meta_file)

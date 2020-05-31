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
from datetime import datetime
from collections import OrderedDict
from zipfile import ZipFile, ZIP_DEFLATED
import toml
from cevast.utils import make_PEM_filename
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
# TODO make persist_and_clear_storage/clear_storage utility method that will identify blocks itself
# TODO change export to optionally not copy file only return path is exists not persisted
# TODO change insert/detele to block sets - dict with block Sets()


class CertFileDBReadOnly(CertDBReadOnly):
    """
    CertDBReadOnly interface implementation which uses files
    and a file system properties as a storage mechanism.
    """

    _CONF_FILE = '.CertFileDB.toml'

    def __init__(self, storage: str):
        # Get config
        try:
            config_path = os.path.join(os.path.abspath(storage), self._CONF_FILE)
            self._config = toml.load(config_path)
            self._params = self._config['PARAMETERS']
            log.info('Found CertFileDB <%s>:\n%s', config_path, self._config)
        except FileNotFoundError:
            raise ValueError('CertFileDB <{}> does not exists -> call CertFileDB.setup() first'.format(config_path))
        # Init DB instance
        log.info('Initializing %s...', self.__class__.__name__)
        # Set containing all target blocks that will be persisted with commit
        self._to_insert: set = set()
        # Set containing all certificates that will be deleted with commit
        self._to_delete: set = set()
        # Set maintaining all known certificate IDs for better EXISTS performance
        self._cache: set = set()

    def get(self, cert_id: str) -> str:
        loc = self._get_cert_location(cert_id)
        filename = make_PEM_filename(cert_id)
        # Check if certificate exists as a file (in case of open transaction)
        if self._to_insert:
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
        if self._to_insert:
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
        # Check the cache first
        if cert_id in self._cache:
            log.debug('<%s> found in cache', cert_id)
            return True

        loc = self._get_cert_location(cert_id)
        cert_filename = make_PEM_filename(cert_id)
        # Check if certificate exists as a file (in case of open transaction)
        if self._to_insert:
            cert_file = os.path.join(loc, cert_filename)
            if os.path.exists(cert_file):
                log.debug('<%s> exists in open transaction', cert_file)
                self._cache.add(cert_id)
                return True
        # Check if certificate exists in a zipfile
        try:
            zip_file = loc + '.zip'
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

    def _get_cert_location(self, cert_id: str) -> str:
        paths = [cert_id[: 2 + i] for i in range(self._params['structure_level'])]
        return os.path.join(self._params['storage'], *paths)


class CertFileDB(CertDB, CertFileDBReadOnly):
    """
    CertDB interface implementation which uses files
    and a file system properties as a storage mechanism.
    """

    @staticmethod
    def setup(storage_path: str, structure_level: int = 2, cert_format: str = 'PEM',
              desc: str = 'CertFileDB', owner: str = '') -> None:
        """
        Setup CertFileDB storage directory with the given parameters.
        Directory and configuration file CertFileDB.toml is created.
        Raise ValueError for wrong parameters or if DB already exists.
        """
        storage_path = os.path.abspath(storage_path)
        config_path = os.path.join(storage_path, CertFileDB._CONF_FILE)
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

        self._to_insert.add(loc)
        self._cache.add(cert_id)
        log.debug('Certificate %s inserted to %s', cert_id, loc)

    def rollback(self) -> None:
        log.info('Rollback started')
        CertFileDB.clear_storage_block(self._to_insert)
        self._to_insert.clear()
        self._to_delete.clear()
        # Simply clear the whole cache, there is no good way how to remove single certs
        self._cache.clear()
        log.info('Rollback finished')

    def commit(self, cores=1) -> None:
        log.info('Commit started')
        # Handle delete first because sequence matter
        self._delete_certs(self._to_delete)
        log.info('Deleted %d certificates', len(self._to_delete))
        self._to_delete.clear()
        # Now insertion can be safely performed
        if cores > 1:
            # TODO use multiprocessing
            # import multiprocessing as mp
            for target in self._to_insert:
                log.debug('Async: Persisting %s group', target)
                # add persist_and_clean_storage_dir(target in ) to pool
        else:
            for target in self._to_insert:
                log.debug('Persisting %s group', target)
                CertFileDB.persist_and_clear_storage_block(target)

        self._to_insert.clear()
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
                self._to_insert.remove(loc)
                CertFileDB.clear_storage_block(loc)
        else:
            self._to_delete.add(cert_id)

        self._cache.discard(cert_id)

    def _create_cert_location(self, cert_id: str) -> str:
        loc = self._get_cert_location(cert_id)
        # Check if location wasn't already created
        if loc not in self._to_insert:
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

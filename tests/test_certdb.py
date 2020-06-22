"""
This module contains white-box unit tests of CertDB package
"""
# pylint: disable=W0212, C0103, C0302
import sys
import os
import subprocess
import time
import shutil
import string
import random
import unittest
import unittest.mock
from collections import OrderedDict
import toml
from cevast.utils import make_PEM_filename
from cevast.certdb import (
    CertDB,
    CertFileDB,
    CertFileDBReadOnly,
    CertNotAvailableError,
    CertInvalidError,
    CompositeCertDB,
    CompositeCertDBReadOnly,
)

# Helper functions
TEST_DATA_PATH = 'tests/data/'
TEST_CERTS_1 = TEST_DATA_PATH + 'test_certs_1.csv'
TEST_CERTS_2 = TEST_DATA_PATH + 'test_certs_2.csv'


def insert_test_certs(database: CertDB, certs_file: str) -> list:
    """
    Insert certificates from certs_file to database
    Return list of inserted certificates.
    """
    certs = []
    with open(certs_file) as r_file:
        for line in r_file:
            els = [e.strip() for e in line.split(',')]
            database.insert(els[0], els[1])
            certs.append(els[0])

    return certs


def insert_random_certs(database: CertDB, certs_cnt: int) -> list:
    """
    Insert number(certs_cnt) randomly generated certificates to database
    Return list of inserted certificates.
    """

    def random_string(length: int) -> str:
        return ''.join(random.choice(string.ascii_letters) for i in range(length))

    certs = []
    for _ in range(certs_cnt):
        cert_id = random_string(16)
        database.insert(cert_id, random_string(8))
        certs.append(cert_id)

    return certs


def delete_test_certs(database: CertDB, certs_file: str) -> list:
    """
    Delete certificates from certs_file from database
    Return list of deleted certificates.
    """
    certs = []
    with open(certs_file) as r_file:
        for line in r_file:
            els = [e.strip() for e in line.split(',')]
            database.delete(els[0])
            certs.append(els[0])

    return certs


def commit_test_certs(database: CertDB, certs_file: str) -> list:
    """
    Insert and commit certificates from certs_file to database
    Return list of committed certificates.
    """
    certs = insert_test_certs(database, certs_file)
    database.commit()
    return certs


class TestCertFileDBReadOnly(unittest.TestCase):
    """Unit test class of CertFileDBReadOnly class"""

    TEST_STORAGE = 'tests/test_storage'

    def tearDown(self):
        # Clear test storage
        shutil.rmtree(self.TEST_STORAGE, ignore_errors=True)

    def test_setup(self):
        """
        Test implementation of CertFileDBReadOnly setup method
        """
        # Check wrong paramaters
        self.assertRaises(ValueError, CertFileDBReadOnly.setup, self.TEST_STORAGE, 'ass')
        # Setup and check DB
        CertFileDBReadOnly.setup(self.TEST_STORAGE, 5, 'DES', 'Testing DB', 'unittest')
        assert os.path.exists(self.TEST_STORAGE)
        cfg = toml.load(os.path.join(self.TEST_STORAGE, CertFileDBReadOnly.CONF_FILENAME))
        meta = toml.load(os.path.join(self.TEST_STORAGE, CertFileDBReadOnly.META_FILENAME))
        self.assertEqual(cfg['PARAMETERS']['storage'], os.path.abspath(self.TEST_STORAGE))
        self.assertEqual(cfg['PARAMETERS']['structure_level'], 5)
        self.assertEqual(cfg['PARAMETERS']['cert_format'], 'DES')
        self.assertEqual(cfg['PARAMETERS']['maintain_info'], True)
        self.assertEqual(meta['INFO']['description'], 'Testing DB')
        self.assertEqual(meta['INFO']['owner'], 'unittest')
        assert 'compression_method' in cfg['PARAMETERS']
        # Try to setup different DB on the same storage
        self.assertRaises(ValueError, CertFileDB.setup, self.TEST_STORAGE, 1, 'PEM', 'Testing DB 2', 'unittest')

    def test_init(self):
        """
        Test of CertFileDBReadOnly initialization
        """
        self.assertRaises(ValueError, CertFileDBReadOnly, self.TEST_STORAGE)
        CertFileDBReadOnly.setup(self.TEST_STORAGE, structure_level=5)
        # Storage should be now properly initialized
        db = CertFileDBReadOnly(self.TEST_STORAGE)
        self.assertEqual(db._params['structure_level'], 5)
        self.assertEqual(db._params['storage'], os.path.abspath(self.TEST_STORAGE))

    def test_get(self):
        """
        Test implementation of CertDB method GET
        """
        CertFileDBReadOnly.setup(self.TEST_STORAGE, maintain_info=False)
        db = CertFileDB(self.TEST_STORAGE)
        db_ronly = CertFileDBReadOnly(self.TEST_STORAGE)
        fake_cert_id = 'fakecertid'
        # Insert and commit some certificates and try to retrieve them back
        commit_test_certs(db, TEST_CERTS_1)
        with open(TEST_CERTS_1) as r_file:
            for line in r_file:
                cert_id, cert = line.split(',')
                # Certificates should exists - transaction was committed
                self.assertEqual(db_ronly.get(cert_id), cert.strip())
        # Only insert other certificates and try to retrieve them back
        inserted = insert_test_certs(db, TEST_CERTS_2)
        for cert_id in inserted:
            # Certificates should NOT exists - transaction was NOT committed
            self.assertRaises(CertNotAvailableError, db_ronly.get, cert_id)
        # Test fake certificate that doesn't exist
        self.assertRaises(CertNotAvailableError, db_ronly.get, fake_cert_id)

    def test_export(self):
        """
        Test implementation of CertDB method EXPORT
        """

        def test_permission(db, valid_cert_id):
            if not sys.platform.startswith('linux'):
                return  # works only on Linux like systems
            fake_target_dir = 'tests/fake_export'

            os.mkdir(fake_target_dir)
            subprocess.call(['chmod', '-w', fake_target_dir])
            self.assertRaises(PermissionError, db.export, valid_cert_id, fake_target_dir)
            subprocess.call(['chmod', '+w', fake_target_dir])
            os.rmdir(fake_target_dir)

        CertFileDBReadOnly.setup(self.TEST_STORAGE, maintain_info=False)
        db = CertFileDB(self.TEST_STORAGE)
        db_ronly = CertFileDBReadOnly(self.TEST_STORAGE)
        target_dir = self.TEST_STORAGE + '/export'
        os.mkdir(target_dir)
        fake_cert_id = 'fakecertid'
        # Insert and commit some certificates and export them
        commit_test_certs(db, TEST_CERTS_1)
        with open(TEST_CERTS_1) as r_file:
            for line in r_file:
                cert_id, cert = line.split(',')
                expected = os.path.join(target_dir, make_PEM_filename(cert_id))
                self.assertEqual(db_ronly.export(cert_id, target_dir), expected)
                with open(expected) as target:
                    self.assertEqual(target.read(), cert.strip())
                # Check export without unnecessary copying - should copy anyway because persisted
                self.assertEqual(db_ronly.export(cert_id, target_dir, copy_if_exists=False), expected)
        # Tests writing permissions for exporting from zipfile
        test_permission(db_ronly, cert_id)
        # Only insert other certificates and try to retrieve them back
        inserted = insert_test_certs(db, TEST_CERTS_2)
        for cert_id in inserted:
            # Certificates should NOT exists - transaction was NOT committed
            self.assertRaises(CertNotAvailableError, db_ronly.export, cert_id, target_dir)
            self.assertRaises(CertNotAvailableError, db_ronly.export, cert_id, target_dir, False)
        # Test fake certificate that doesn't exist
        self.assertRaises(CertNotAvailableError, db_ronly.export, fake_cert_id, target_dir)

    def test_exists(self):
        """
        Test implementation of CertDB method EXISTS
        """
        CertFileDBReadOnly.setup(self.TEST_STORAGE, maintain_info=False)
        db = CertFileDB(self.TEST_STORAGE)
        db_ronly = CertFileDBReadOnly(self.TEST_STORAGE)
        fake_cert = 'fakecertid'
        # Insert and commit some certificates and check if exists
        committed = commit_test_certs(db, TEST_CERTS_1)
        for cert in committed:
            assert db_ronly.exists(cert)
        assert db_ronly.exists_all(committed)
        # Only insert other certificates and check if exists
        inserted = insert_test_certs(db, TEST_CERTS_2)
        for cert in inserted:
            assert not db_ronly.exists(cert)
        assert not db_ronly.exists_all(inserted)
        # Test fake certificate that doesn't exist
        committed.append(fake_cert)
        assert not db_ronly.exists(fake_cert)
        assert not db_ronly.exists_all(committed)

    def test_cache(self):
        """
        Test implementation of CertFileDB certificate existance cache
        """
        CertFileDB.setup(self.TEST_STORAGE, maintain_info=False)
        db = CertFileDB(self.TEST_STORAGE)
        db_ronly = CertFileDBReadOnly(self.TEST_STORAGE)
        # Insert and commit some certificates and check cache
        committed = commit_test_certs(db, TEST_CERTS_1)
        for cert in committed:
            assert cert not in db_ronly._cache
            db_ronly.exists(cert)
            assert cert in db_ronly._cache
        self.assertEqual(db_ronly._cache, set(committed))

        # Insert and commit some certificates and check cache after exists_all call
        committed = commit_test_certs(db, TEST_CERTS_2)
        assert not set(committed).issubset(db_ronly._cache)
        db_ronly.exists_all(committed)
        assert set(committed).issubset(db_ronly._cache)

        # Check DELETE effect on cache
        db.exists_all(committed)
        self.assertEqual(set(committed), db._cache)
        db.delete(committed[0])
        assert committed[0] not in db._cache
        self.assertNotEqual(set(committed), db._cache)
        db.rollback()

        # Check speed improvement using cache - on large number of certs
        inserted = insert_random_certs(db, 1000)
        db.commit()
        t0 = time.clock()
        for cert in inserted:
            db_ronly.exists(cert)
        t1 = time.clock()
        for cert in inserted:
            db_ronly.exists(cert)
        t2 = time.clock()
        self.assertGreater(t1 - t0, t2 - t1)


class TestCertFileDB(unittest.TestCase):
    """Unit test class of CertFileDB class"""

    TEST_STORAGE = 'tests/test_storage'

    def tearDown(self):
        # Clear test storage
        shutil.rmtree(self.TEST_STORAGE, ignore_errors=True)
        if os.path.exists(self.TEST_STORAGE + '.zip'):
            os.remove(self.TEST_STORAGE + '.zip')

    def test_init(self):
        """
        Test of CertFileDB initialization
        """
        self.assertRaises(ValueError, CertFileDB, self.TEST_STORAGE)
        CertFileDB.setup(self.TEST_STORAGE, structure_level=5)
        # Storage should be now properly initialized
        db = CertFileDB(self.TEST_STORAGE)
        self.assertEqual(db._params['structure_level'], 5)
        self.assertEqual(db._params['storage'], os.path.abspath(self.TEST_STORAGE))

    def test_get(self):
        """
        Test implementation of CertDB method GET
        """
        CertFileDB.setup(self.TEST_STORAGE, maintain_info=False)
        db = CertFileDB(self.TEST_STORAGE)
        fake_cert_id = 'fakecertid'
        # Insert and commit some certificates and retrieve them back
        committed = commit_test_certs(db, TEST_CERTS_1)
        with open(TEST_CERTS_1) as r_file:
            for line in r_file:
                cert_id, cert = line.split(',')
                self.assertEqual(db.get(cert_id), cert.strip())

        # Only insert other certificates and retrieve them back
        inserted = insert_test_certs(db, TEST_CERTS_2)
        with open(TEST_CERTS_2) as r_file:
            for line in r_file:
                cert_id, cert = line.split(',')
                self.assertEqual(db.get(cert_id), cert.strip())
            # Rollback and try to retrieve them again
            db.rollback()
            for cert_id in inserted:
                self.assertRaises(CertNotAvailableError, db.get, cert_id)

        # Test DELETE method effect
        db.delete(committed[0])
        self.assertRaises(CertNotAvailableError, db.get, committed[0])

        # Test fake certificate that doesn't exist
        self.assertRaises(CertNotAvailableError, db.get, fake_cert_id)

    def test_export(self):
        """
        Test implementation of CertDB method EXPORT
        """

        def test_permission(db, valid_cert_id):
            if not sys.platform.startswith('linux'):
                return  # works only on Linux like systems
            fake_target_dir = 'tests/fake_export'
            os.mkdir(fake_target_dir)
            subprocess.call(['chmod', '-w', fake_target_dir])
            self.assertRaises(PermissionError, db.export, valid_cert_id, fake_target_dir)
            subprocess.call(['chmod', '+w', fake_target_dir])
            os.rmdir(fake_target_dir)

        CertFileDB.setup(self.TEST_STORAGE, maintain_info=False)
        db = CertFileDB(self.TEST_STORAGE)
        target_dir = self.TEST_STORAGE + '/export'
        os.mkdir(target_dir)
        fake_cert_id = 'fakecertid'

        # Insert and commit some certificates and export them
        committed = commit_test_certs(db, TEST_CERTS_1)
        with open(TEST_CERTS_1) as r_file:
            for line in r_file:
                cert_id, cert = line.split(',')
                expected = os.path.join(target_dir, make_PEM_filename(cert_id))
                self.assertEqual(db.export(cert_id, target_dir), expected)
                with open(expected) as target:
                    self.assertEqual(target.read(), cert.strip())
                # Check export without unnecessary copying - should copy anyway because persisted
                self.assertEqual(db.export(cert_id, target_dir, copy_if_exists=False), expected)
        # Tests writing permissions for exporting from zipfile
        test_permission(db, cert_id)

        # Only insert other certificates and retrieve them back
        insert_test_certs(db, TEST_CERTS_2)
        with open(TEST_CERTS_2) as r_file:
            for line in r_file:
                cert_id, cert = line.split(',')
                expected = os.path.join(target_dir, make_PEM_filename(cert_id))
                self.assertEqual(db.export(cert_id, target_dir), expected)
                with open(expected) as target:
                    self.assertEqual(target.read(), cert.strip())
                # Check export without unnecessary copying
                file = db.export(cert_id, target_dir, copy_if_exists=False)
                self.assertNotEqual(file, expected)
                with open(file) as target:
                    self.assertEqual(target.read(), cert.strip())
            # Tests writing permissions for exporting from transaction
            test_permission(db, cert_id)
            # Rollback and try to retrieve them again
            db.rollback()
            r_file.seek(0)
            for line in r_file:
                cert_id = line.split(',')[0]
                self.assertRaises(CertNotAvailableError, db.export, cert_id, target_dir)

        # Test DELETE method effect
        db.delete(committed[0])
        self.assertRaises(CertNotAvailableError, db.get, committed[0])

        # Test fake certificate that doesn't exist
        self.assertRaises(CertNotAvailableError, db.export, fake_cert_id, target_dir)

    def test_exists(self):
        """
        Test implementation of CertDB method EXISTS
        """
        CertFileDB.setup(self.TEST_STORAGE, maintain_info=False)
        db = CertFileDB(self.TEST_STORAGE)
        fake_cert = 'fakecertid'
        # Insert and commit some certificates and check if exists
        committed = commit_test_certs(db, TEST_CERTS_1)
        for cert in committed:
            assert db.exists(cert)
        assert db.exists_all(committed)

        # Only insert other certificates and check if exists
        inserted = insert_test_certs(db, TEST_CERTS_2)
        for cert in inserted:
            assert db.exists(cert)
        assert db.exists_all(inserted)

        # Test DELETE method effect
        db.delete(committed[0])
        assert not db.exists(committed[0])

        # Test fake certificate that doesn't exist
        committed.append(fake_cert)
        assert not db.exists(fake_cert)
        assert not db.exists_all(committed)

    def test_insert(self):
        """
        Test implementation of CertDB method INSERT
        """
        CertFileDB.setup(self.TEST_STORAGE, maintain_info=False)
        db = CertFileDB(self.TEST_STORAGE)
        # Insert some invalid certificates
        self.assertRaises(CertInvalidError, db.insert, None, None)
        self.assertRaises(CertInvalidError, db.insert, '', '')
        self.assertRaises(CertInvalidError, db.insert, '', 'valid')
        self.assertRaises(CertInvalidError, db.insert, 'valid', None)

        # Insert some valid certificates
        inserted = insert_test_certs(db, TEST_CERTS_1)
        blocks = {**db._to_insert}
        # transaction should contain certificates from open transcation and certs should exist
        self.assertTrue(db._to_insert)
        for cert in inserted:
            block_path = db._get_block_path(cert)
            assert os.path.exists(os.path.join(block_path, cert))

        # Insert different certificates under the same IDs
        certs = {}
        with open(TEST_CERTS_1) as r_file:
            for line in r_file:
                els = [e.strip() for e in line.split(',')]
                db.insert(els[0], els[1] + '_open')
                certs[els[0]] = els[1]
        # IDs should be same and certificates should not be changed
        self.assertTrue(blocks == db._to_insert)
        for k, v in certs.items():
            self.assertTrue(db.get(k) == v)

        # Commit transaction and commit different certificates under the same IDs
        db.commit()
        self.assertFalse(db._to_insert)
        certs = {}
        with open(TEST_CERTS_1) as r_file:
            for line in r_file:
                els = [el.strip() for el in line.split(',')]
                db.insert(els[0], els[1] + '_commit')
                certs[els[0]] = els[1]
        # IDs should be same and persisted certificates should not be changed
        self.assertTrue(blocks == db._to_insert)
        db.commit()
        self.assertFalse(db._to_insert)
        for k, v in certs.items():
            self.assertTrue(db.get(k) == v)

    def test_delete(self):
        """
        Test implementation of CertDB method DELETE
        """
        CertFileDB.setup(self.TEST_STORAGE, maintain_info=False)
        db = CertFileDB(self.TEST_STORAGE)
        # Delete some invalid certificates
        self.assertRaises(CertInvalidError, db.delete, None)
        self.assertRaises(CertInvalidError, db.delete, '')

        # Insert and delete the same certs before commit
        inserted = insert_test_certs(db, TEST_CERTS_1)
        deleted = delete_test_certs(db, TEST_CERTS_1)
        # transaction should be clear and files should not exist
        self.assertFalse(db._to_delete)
        self.assertFalse(db._to_insert)
        for cert in inserted:
            block_path = db._get_block_path(cert)
            assert not os.path.exists(os.path.join(block_path, cert))

        # Delete and insert the same certs before commit
        deleted = delete_test_certs(db, TEST_CERTS_1)
        inserted = insert_test_certs(db, TEST_CERTS_1)
        # transaction should contain deleted and inserted certificates
        self.assertTrue(db._to_delete)
        self.assertTrue(db._to_insert)
        for certs in db._to_delete.values():
            assert certs.issubset(set(deleted))
        for certs in db._to_insert.values():
            assert certs.issubset(set(inserted))
        # and files should exist
        for cert in inserted:
            block_path = db._get_block_path(cert)
            assert os.path.exists(os.path.join(block_path, cert))
        # now commit and check that files were persisted
        ins, dlt = db.commit()
        # the certs should be only inserted
        self.assertEqual(ins, len(inserted))
        self.assertEqual(dlt, 0)
        self.assertFalse(db._to_delete)
        self.assertFalse(db._to_insert)

        # Delete inserted certs, commit and check that they were deleted
        assert db.exists_all(inserted)
        del_cert = inserted.pop()
        db.delete(del_cert)
        assert not db.exists(del_cert)
        db.commit()
        assert not db.exists(del_cert)
        for cert in inserted:
            db.delete(cert)
        ins, dlt = db.commit()
        self.assertEqual(ins, 0)
        self.assertEqual(dlt, len(inserted))
        # storage should be empty
        self.assertFalse(os.listdir(db.storage).remove(db.CONF_FILENAME))

        # Delete the same cert multiple times should not have effect
        self.assertFalse(db._to_delete)
        db.delete('validcert')
        blocks_to_delete = {**db._to_delete}
        self.assertTrue(db._to_delete)
        db.delete('validcert')
        self.assertTrue(db._to_delete)
        self.assertEqual(blocks_to_delete, db._to_delete)

    def test_rollback(self):
        """
        Test implementation of CertDB method ROLLBACK
        """
        CertFileDB.setup(self.TEST_STORAGE, maintain_info=False)
        db = CertFileDB(self.TEST_STORAGE)
        # Test rollback without inserts
        db.rollback()
        self.assertFalse(db._to_insert)
        self.assertFalse(db._to_delete)

        # Insert some certificates, rollback and check that blocks are deleted
        inserted = insert_test_certs(db, TEST_CERTS_1)
        db.rollback()
        for cert in inserted:
            block_path = db._get_block_path(cert)
            assert not os.path.exists(os.path.join(block_path, cert))
        # Transaction should be empty
        self.assertFalse(db._to_insert)

        # Commit some certs, insert other certs and rollback
        committed = commit_test_certs(db, TEST_CERTS_1)
        inserted = insert_test_certs(db, TEST_CERTS_2)
        db.rollback()
        # Transaction should be empty
        self.assertFalse(db._to_insert)
        # Commited certs should be compressed in zip files
        for cert in committed:
            assert not os.path.exists(db._get_block_path(cert) + cert)
            assert os.path.exists(db._get_block_archive(cert))
        # Rollbacked certs files should not exists
        for cert in inserted:
            block_path = db._get_block_path(cert)
            assert not os.path.exists(os.path.join(block_path, cert))

        # Check rollback of delete method
        deleted = delete_test_certs(db, TEST_CERTS_1)
        self.assertTrue(db._to_delete)
        for cert in deleted:
            assert not db.exists(cert)
        db.rollback()
        self.assertFalse(db._to_delete)
        # All deleted certs should still exist
        assert db.exists_all(deleted)

    def test_commit(self):
        """
        Test implementation of CertDB method COMMIT
        """
        CertFileDB.setup(self.TEST_STORAGE, maintain_info=False)
        db = CertFileDB(self.TEST_STORAGE)
        # Test commit without inserts
        ins, dlt = db.commit()
        self.assertEqual(ins, 0)
        self.assertEqual(dlt, 0)
        self.assertFalse(db._to_insert)

        # Insert some certificates and check commit
        inserted = insert_test_certs(db, TEST_CERTS_1)
        # Certificates and blocks from open transaction should exist
        self.assertTrue(db._to_insert)
        for certs in db._to_insert.values():
            assert certs.issubset(set(inserted))
        for cert in inserted:
            block_path = db._get_block_path(cert)
            assert os.path.exists(os.path.join(block_path, cert))
        # check correct number of committed certs
        ins, dlt = db.commit()
        self.assertEqual(ins, len(inserted))
        self.assertEqual(dlt, 0)
        # transaction should be empty and certs should be compressed in zip files
        self.assertFalse(db._to_insert)
        for cert in inserted:
            assert not os.path.exists(db._get_block_path(cert) + cert)
            assert os.path.exists(db._get_block_archive(cert))

        # Insert already persisted certs and some others and commit
        inserted_again = insert_test_certs(db, TEST_CERTS_1)
        inserted_new = insert_test_certs(db, TEST_CERTS_2)
        ins, dlt = db.commit()
        # only the other certs should be committed
        self.assertEqual(ins, len(inserted_new))
        self.assertEqual(dlt, 0)
        # and the same ones should be deleted from transaction
        for cert in inserted_again:
            block_path = db._get_block_path(cert)
            assert not os.path.exists(os.path.join(block_path, cert))

        # Delete and insert the same not yet persisted cert and commit
        valid_cert = ['valid_cert', 'validvalidvalidvalidvalid']
        db.delete(valid_cert[0])
        db.insert(*valid_cert)
        db.commit()
        # check that cert is persisted
        assert db.exists(valid_cert[0])
        assert os.path.exists(db._get_block_archive(valid_cert[0]))
        assert not os.path.exists(db._get_block_path(valid_cert[0]) + valid_cert[0])

        # Delete and insert the same already persisted cert and commit
        valid_cert = ['valid_cert', 'validvalidvalidvalidvalid_new']
        db.delete(valid_cert[0])
        db.insert(*valid_cert)
        db.commit()
        # check that the cert was replaced
        assert db.exists(valid_cert[0])
        self.assertEqual(db.get(valid_cert[0]), valid_cert[1])

    def test_parallel_transactions(self):
        """
        Test of using multiple instances of CertDB with the same storage.
        """

    def test_config_info_maintain(self):
        """
        Test maintaining commit HISTORY and INFO upon commit
        """
        CertFileDB.setup(self.TEST_STORAGE, maintain_info=True)
        db = CertFileDB(self.TEST_STORAGE)
        meta_path = os.path.join(db.storage, db.META_FILENAME)
        # Insert some certificates and check INFO after commit
        committed = commit_test_certs(db, TEST_CERTS_1)
        meta = toml.load(meta_path, OrderedDict)
        last_commit_nr = str(len(meta['HISTORY']))
        self.assertEqual(last_commit_nr, '1')
        self.assertEqual(meta['INFO']['number_of_certificates'], len(committed))
        self.assertEqual(meta['INFO']['last_commit'], meta['HISTORY'][last_commit_nr]['date'])
        self.assertEqual(meta['HISTORY'][last_commit_nr]['inserted'], len(committed))
        self.assertEqual(meta['HISTORY'][last_commit_nr]['deleted'], 0)

        # Delete all the inserted certs and check INFO after commit
        deleted = delete_test_certs(db, TEST_CERTS_1)
        db.commit()
        meta = toml.load(meta_path, OrderedDict)
        last_commit_nr = str(len(meta['HISTORY']))
        self.assertEqual(last_commit_nr, '2')
        self.assertEqual(meta['INFO']['number_of_certificates'], 0)
        self.assertEqual(meta['INFO']['last_commit'], meta['HISTORY'][last_commit_nr]['date'])
        self.assertEqual(meta['HISTORY'][last_commit_nr]['inserted'], 0)
        self.assertEqual(meta['HISTORY'][last_commit_nr]['deleted'], len(deleted))

        # Insert and delete some certs and check INFO after commit
        committed = commit_test_certs(db, TEST_CERTS_1)
        inserted = insert_test_certs(db, TEST_CERTS_2)
        deleted = delete_test_certs(db, TEST_CERTS_1)
        db.commit()
        meta = toml.load(meta_path, OrderedDict)
        last_commit_nr = str(len(meta['HISTORY']))
        self.assertEqual(last_commit_nr, '4')
        self.assertEqual(meta['INFO']['number_of_certificates'], len(inserted))
        self.assertEqual(meta['INFO']['last_commit'], meta['HISTORY'][last_commit_nr]['date'])
        self.assertEqual(meta['HISTORY'][last_commit_nr]['inserted'], len(inserted))
        self.assertEqual(meta['HISTORY'][last_commit_nr]['deleted'], len(deleted))

    def test_zero_structure_level(self):
        """
        Test CertFileDB with 0 structure_level
        """
        CertFileDB.setup(self.TEST_STORAGE, structure_level=0)
        db = CertFileDB(self.TEST_STORAGE)
        storage_dir = os.path.join(self.TEST_STORAGE, os.path.basename(self.TEST_STORAGE))
        # Commit some certificates and check zipfile
        committed = commit_test_certs(db, TEST_CERTS_1)
        assert db.exists_all(committed)
        assert os.path.exists(storage_dir + '.zip')
        # Insert some certificates and check files existance in root folder
        inserted = insert_test_certs(db, TEST_CERTS_2)
        for cert in inserted:
            assert os.path.exists(os.path.join(self.TEST_STORAGE, cert))
            assert db.exists(cert)
        assert db.exists_all(inserted)
        # Rollback check file cleanup
        db.rollback()
        for cert in inserted:
            assert not os.path.exists(os.path.join(storage_dir, cert))
            assert not db.exists(cert)
        # Delete inserted certificates and check file cleanup
        inserted = insert_test_certs(db, TEST_CERTS_2)
        delete_test_certs(db, TEST_CERTS_2)
        for cert in inserted:
            assert not os.path.exists(os.path.join(storage_dir, cert))
            assert not db.exists(cert)
        self.assertFalse(db._to_insert)
        self.assertFalse(db._to_delete)
        # Retrieve and check persisted certs
        with open(TEST_CERTS_1) as r_file:
            for line in r_file:
                cert_id, cert = line.split(',')
                self.assertEqual(db.get(cert_id), cert.strip())
        # Delete all remaining certificates and check zip cleanup
        deleted = delete_test_certs(db, TEST_CERTS_1)
        db.commit()
        for cert in deleted:
            assert not os.path.exists(os.path.join(storage_dir, cert))
            assert not db.exists(cert)
        assert not os.path.exists(storage_dir + '.zip')

    def test_async_commit(self):
        """
        Test implementation multiprocessing version of CertDB method COMMIT
        """
        CertFileDB.setup(self.TEST_STORAGE, maintain_info=False)
        db = CertFileDB(self.TEST_STORAGE, 100)
        # Test commit without inserts
        ins, dlt = db.commit()
        self.assertEqual(ins, 0)
        self.assertEqual(dlt, 0)
        self.assertFalse(db._to_insert)

        # Insert some certificates and check commit
        inserted = insert_test_certs(db, TEST_CERTS_1)
        # Certificates and blocks from open transaction should exist
        for cert in inserted:
            block_path = db._get_block_path(cert)
            assert os.path.exists(os.path.join(block_path, cert))
        # check correct number of committed certs
        ins, dlt = db.commit()
        self.assertEqual(ins, len(inserted))
        self.assertEqual(dlt, 0)
        # transaction should be empty and certs should be compressed in zip files
        self.assertFalse(db._to_insert)
        for cert in inserted:
            assert not os.path.exists(db._get_block_path(cert) + cert)
            assert os.path.exists(db._get_block_archive(cert))

        # Insert already persisted certs and some others and commit
        inserted_again = insert_test_certs(db, TEST_CERTS_1)
        inserted_new = insert_test_certs(db, TEST_CERTS_2)
        ins, dlt = db.commit()
        # only the other certs should be committed
        self.assertEqual(ins, len(inserted_new))
        self.assertEqual(dlt, 0)
        # and the same ones should be deleted from transaction
        for cert in inserted_again:
            block_path = db._get_block_path(cert)
            assert not os.path.exists(os.path.join(block_path, cert))

        # Delete and insert the same not yet persisted cert and commit
        valid_cert = ['valid_cert', 'validvalidvalidvalidvalid']
        db.delete(valid_cert[0])
        db.insert(*valid_cert)
        db.commit()
        # check that cert is persisted
        assert db.exists(valid_cert[0])
        assert os.path.exists(db._get_block_archive(valid_cert[0]))
        assert not os.path.exists(db._get_block_path(valid_cert[0]) + valid_cert[0])

        # Delete and insert the same already persisted cert and commit
        valid_cert = ['valid_cert', 'validvalidvalidvalidvalid_new']
        db.delete(valid_cert[0])
        db.insert(*valid_cert)
        db.commit()
        # check that the cert was replaced
        assert db.exists(valid_cert[0])
        self.assertEqual(db.get(valid_cert[0]), valid_cert[1])


class TestCompositeCertDB(unittest.TestCase):
    """Unit test class of CompositeCertDB class"""

    TEST_STORAGE_1 = 'tests/test_storage1'
    TEST_STORAGE_2 = 'tests/test_storage2'
    TEST_STORAGE_3 = 'tests/test_storage3'

    def tearDown(self):
        # Clear test storage
        shutil.rmtree(self.TEST_STORAGE_1, ignore_errors=True)
        shutil.rmtree(self.TEST_STORAGE_2, ignore_errors=True)
        shutil.rmtree(self.TEST_STORAGE_3, ignore_errors=True)

    def setUp(self):
        CertFileDB.setup(self.TEST_STORAGE_1)
        CertFileDB.setup(self.TEST_STORAGE_2)
        CertFileDB.setup(self.TEST_STORAGE_3)

    def test_component_management(self):
        """
        Test implementation of CompositeCertDB management methods and design
        """
        valid_cert = 'validcertid'
        real_db = CertFileDBReadOnly(self.TEST_STORAGE_1)
        composite_db_read_only = CompositeCertDBReadOnly()
        composite_db = CompositeCertDB()
        # Mock method EXISTS
        real_db.exists = unittest.mock.MagicMock()
        real_db.exists.return_value = False
        # Check register/unregister method
        composite_db_read_only.register(real_db)
        assert not composite_db_read_only.exists(valid_cert)
        assert composite_db_read_only.is_registered(real_db)
        # component's EXISTS method should be executed
        real_db.exists.assert_called_once_with(valid_cert)
        composite_db_read_only.unregister(real_db)
        # component's EXISTS method should NOT be executed
        assert not composite_db_read_only.exists(valid_cert)
        self.assertEqual(real_db.exists.call_count, 1)
        assert not composite_db_read_only.is_registered(real_db)

        # Check registering the same object twice
        composite_db_read_only.register(real_db)
        composite_db_read_only.register(real_db)
        assert not composite_db_read_only.exists(valid_cert)
        self.assertEqual(real_db.exists.call_count, 2)
        assert composite_db_read_only.is_registered(real_db)

        # Check unregistering unknown object
        composite_db.unregister(real_db)
        assert not composite_db.is_registered(real_db)
        assert not composite_db.exists(valid_cert)

        # Check registering composite DB into another composite DB
        self.assertEqual(real_db.exists.call_count, 2)
        composite_db.register(real_db)
        composite_db.register(composite_db_read_only)
        assert not composite_db.exists(valid_cert)
        self.assertEqual(real_db.exists.call_count, 4)
        assert composite_db.is_registered(real_db)
        assert composite_db.is_registered(composite_db_read_only)
        assert composite_db_read_only.is_registered(real_db)

    def test_combine_read_only(self):
        """
        Test implementation of CompositeCertDB management with mixed component types
        """
        valid_cert = ('validcertid', 'adadadadadadadadada')
        real_db = CertFileDB(self.TEST_STORAGE_1)
        real_db_read_only = CertFileDBReadOnly(self.TEST_STORAGE_2)
        composite_db = CompositeCertDB()
        # Mock method EXISTS and INSERT
        real_db.insert = unittest.mock.MagicMock()
        real_db_read_only.insert = unittest.mock.MagicMock()
        real_db.exists = unittest.mock.MagicMock()
        real_db.exists.return_value = False
        real_db_read_only.exists = unittest.mock.MagicMock()
        real_db_read_only.exists.return_value = False

        # Register both DBs to composite DB and call EXISTS
        composite_db.register(real_db)
        composite_db.register(real_db_read_only)
        assert not composite_db.exists(valid_cert[0])
        # both component's EXISTS method should be executed
        real_db.exists.assert_called_once_with(valid_cert[0])
        real_db_read_only.exists.assert_called_once_with(valid_cert[0])

        # Call INSERT and check that only CertFileDB was executed
        composite_db.insert(*valid_cert)
        real_db.insert.assert_called_once_with(*valid_cert)
        assert not real_db_read_only.insert.called

    def test_get(self):
        """
        Test implementation of CompositeCertDB method GET
        """
        real_db = CertFileDB(self.TEST_STORAGE_1)
        real_db2 = CertFileDB(self.TEST_STORAGE_2)
        real_db_read_only = CertFileDBReadOnly(self.TEST_STORAGE_1)
        composite_db = CompositeCertDB()
        composite_db.register(real_db)
        composite_db.register(real_db2)
        composite_db.register(real_db_read_only)
        fake_cert_id = 'fakecertid'
        # Insert and commit some certificates and retrieve them back
        committed = commit_test_certs(composite_db, TEST_CERTS_1)
        with open(TEST_CERTS_1) as r_file:
            for line in r_file:
                cert_id, cert = line.split(',')
                self.assertEqual(composite_db.get(cert_id), cert.strip())
                # ReadOnly DB should also have it
                self.assertEqual(real_db_read_only.get(cert_id), cert.strip())

        # Only insert other certificates and retrieve them back
        inserted = insert_test_certs(composite_db, TEST_CERTS_2)
        with open(TEST_CERTS_2) as r_file:
            for line in r_file:
                cert_id, cert = line.split(',')
                self.assertEqual(composite_db.get(cert_id), cert.strip())
                # ReadOnly DB should not have it
                self.assertRaises(CertNotAvailableError, real_db_read_only.get, cert_id)
            # Rollback and try to retrieve them again
            composite_db.rollback()
            for cert_id in inserted:
                self.assertRaises(CertNotAvailableError, composite_db.get, cert_id)

        # Test DELETE method effect
        real_db.delete(committed[0])
        # compositeDB should still have it in real_db2
        assert composite_db.get(committed[0])
        composite_db.delete(committed[0])
        # compositeDB should still have it in real_db_read_only before commit
        assert composite_db.get(committed[0])
        composite_db.commit()
        # compositeDB should no longer have the cert
        self.assertRaises(CertNotAvailableError, composite_db.get, committed[0])

        # Test fake certificate that doesn't exist
        self.assertRaises(CertNotAvailableError, composite_db.get, fake_cert_id)

    def test_export(self):
        """
        Test implementation of CompositeCertDB method EXPORT
        """
        real_db = CertFileDB(self.TEST_STORAGE_1)
        real_db2 = CertFileDB(self.TEST_STORAGE_2)
        real_db_read_only = CertFileDBReadOnly(self.TEST_STORAGE_1)
        composite_db = CompositeCertDB()
        composite_db.register(real_db)
        composite_db.register(real_db2)
        composite_db.register(real_db_read_only)
        fake_cert_id = 'fakecertid'
        target_dir = self.TEST_STORAGE_1 + '/export'
        os.mkdir(target_dir)
        # Insert and commit some certificates and export them
        committed = commit_test_certs(composite_db, TEST_CERTS_1)
        with open(TEST_CERTS_1) as r_file:
            for line in r_file:
                cert_id, cert = line.split(',')
                expected = os.path.join(target_dir, make_PEM_filename(cert_id))
                self.assertEqual(composite_db.export(cert_id, target_dir), expected)
                with open(expected) as target:
                    self.assertEqual(target.read(), cert.strip())
                # Check export without unnecessary copying - should copy anyway because persisted
                self.assertEqual(composite_db.export(cert_id, target_dir, copy_if_exists=False), expected)
                # ReadOnly DB should also have it
                self.assertEqual(real_db_read_only.export(cert_id, target_dir), expected)

        # Only insert other certificates and retrieve them back
        insert_test_certs(composite_db, TEST_CERTS_2)
        with open(TEST_CERTS_2) as r_file:
            for line in r_file:
                cert_id, cert = line.split(',')
                expected = os.path.join(target_dir, make_PEM_filename(cert_id))
                self.assertEqual(composite_db.export(cert_id, target_dir), expected)
                with open(expected) as target:
                    self.assertEqual(target.read(), cert.strip())
                # Check export without unnecessary copying
                file = composite_db.export(cert_id, target_dir, copy_if_exists=False)
                self.assertNotEqual(file, expected)
                with open(file) as target:
                    self.assertEqual(target.read(), cert.strip())
                # ReadOnly DB should not have it
                self.assertRaises(CertNotAvailableError, real_db_read_only.export, cert_id, target_dir)
            # Rollback and try to retrieve them again
            composite_db.rollback()
            r_file.seek(0)
            for line in r_file:
                cert_id = line.split(',')[0]
                self.assertRaises(CertNotAvailableError, composite_db.export, cert_id, target_dir)

        # Test DELETE method effect
        real_db.delete(committed[0])
        # compositeDB should still have it in real_db2
        assert composite_db.export(committed[0], target_dir)
        composite_db.delete(committed[0])
        # compositeDB should still have it in real_db_read_only before commit
        assert composite_db.export(committed[0], target_dir)
        composite_db.commit()
        # compositeDB should no longer have the cert
        self.assertRaises(CertNotAvailableError, composite_db.export, committed[0], target_dir)

        # Test fake certificate that doesn't exist
        self.assertRaises(CertNotAvailableError, composite_db.export, fake_cert_id, target_dir)

    def test_exists(self):
        """
        Test implementation of CompositeCertDB method EXISTS
        """
        real_db = CertFileDB(self.TEST_STORAGE_1)
        real_db2 = CertFileDB(self.TEST_STORAGE_2)
        real_db_read_only = CertFileDBReadOnly(self.TEST_STORAGE_1)
        composite_db = CompositeCertDB()
        composite_db.register(real_db)
        composite_db.register(real_db2)
        composite_db.register(real_db_read_only)
        fake_cert = 'fakecertid'
        # Insert and commit some certificates and check if exists
        committed = commit_test_certs(composite_db, TEST_CERTS_1)
        for cert in committed:
            assert composite_db.exists(cert)
            # ReadOnly DB should also have it
            assert real_db_read_only.exists(cert)
        assert composite_db.exists_all(committed)

        # Only insert other certificates and check if exists
        inserted = insert_test_certs(composite_db, TEST_CERTS_2)
        for cert in inserted:
            assert composite_db.exists(cert)
            # ReadOnly DB should NOT have it
            assert not real_db_read_only.exists(cert)
        assert composite_db.exists_all(inserted)

        # Test DELETE method effect
        real_db.delete(committed[0])
        # compositeDB should still have it in real_db2
        assert composite_db.exists(committed[0])
        composite_db.delete(committed[0])
        # compositeDB should still have it in real_db_read_only before commit
        assert composite_db.exists(committed[0])
        composite_db.commit()
        # compositeDB should no longer have the cert but cache in real_db_read_only have
        assert not real_db.exists(committed[0])
        assert not real_db2.exists(committed[0])
        # get method upon failure should clear the cache if seems invalidated
        self.assertRaises(CertNotAvailableError, real_db_read_only.get, committed[0])
        assert not real_db_read_only.exists(committed[0])

        # Have 1 cert in one DB and other cert in other DB and check EXISTS method
        real_db.delete(committed[2])
        assert not real_db.exists(committed[2])
        real_db2.delete(committed[3])
        assert not real_db2.exists(committed[3])
        # composite_db should return True
        assert composite_db.exists(committed[2])
        assert composite_db.exists(committed[3])
        assert composite_db.exists_all([committed[2], committed[3]])

        # Test fake certificate that doesn't exist
        committed.append(fake_cert)
        assert not composite_db.exists(fake_cert)
        assert not composite_db.exists_all(committed)

    def test_insert(self):
        """
        Test implementation of CompositeCertDB method INSERT
        """
        real_db = CertFileDB(self.TEST_STORAGE_1)
        real_db2 = CertFileDB(self.TEST_STORAGE_2)
        composite_db = CompositeCertDB()
        composite_db.register(real_db)
        composite_db.register(real_db2)
        # Insert some invalid certificates
        self.assertRaises(CertInvalidError, composite_db.insert, None, None)
        self.assertRaises(CertInvalidError, composite_db.insert, '', '')
        self.assertRaises(CertInvalidError, composite_db.insert, '', 'valid')
        self.assertRaises(CertInvalidError, composite_db.insert, 'valid', None)

        # Insert some valid certificates
        inserted = insert_test_certs(composite_db, TEST_CERTS_1)
        blocks = {**real_db._to_insert}
        blocks2 = {**real_db2._to_insert}
        # transaction should contain certificates from open transcation and certs should exist
        self.assertTrue(real_db._to_insert)
        self.assertTrue(real_db2._to_insert)
        for cert in inserted:
            block_path = real_db._get_block_path(cert)
            block_path2 = real_db2._get_block_path(cert)
            assert os.path.exists(os.path.join(block_path, cert))
            assert os.path.exists(os.path.join(block_path2, cert))

        # Insert different certificates under the same IDs
        certs = {}
        with open(TEST_CERTS_1) as r_file:
            for line in r_file:
                els = [e.strip() for e in line.split(',')]
                composite_db.insert(els[0], els[1] + '_open')
                certs[els[0]] = els[1]
        # IDs should be same and certificates should not be changed
        self.assertTrue(blocks == real_db._to_insert)
        self.assertTrue(blocks2 == real_db2._to_insert)
        for k, v in certs.items():
            self.assertTrue(real_db.get(k) == v)
            self.assertTrue(real_db2.get(k) == v)

        # Commit transaction and commit different certificates under the same IDs
        composite_db.commit()
        self.assertFalse(real_db._to_insert)
        self.assertFalse(real_db2._to_insert)
        certs = {}
        with open(TEST_CERTS_1) as r_file:
            for line in r_file:
                els = [el.strip() for el in line.split(',')]
                composite_db.insert(els[0], els[1] + '_commit')
                certs[els[0]] = els[1]
        # IDs should be same and persisted certificates should not be changed
        self.assertTrue(blocks == real_db._to_insert)
        self.assertTrue(blocks2 == real_db2._to_insert)
        composite_db.commit()
        self.assertFalse(real_db._to_insert)
        self.assertFalse(real_db2._to_insert)
        for k, v in certs.items():
            self.assertTrue(real_db.get(k) == v)
            self.assertTrue(real_db2.get(k) == v)

    def test_delete(self):
        """
        Test implementation of CompositeCertDB method DELETE
        """
        real_db = CertFileDB(self.TEST_STORAGE_1)
        real_db2 = CertFileDB(self.TEST_STORAGE_2)
        composite_db = CompositeCertDB()
        composite_db.register(real_db)
        composite_db.register(real_db2)
        # Delete some invalid certificates
        self.assertRaises(CertInvalidError, composite_db.delete, None)
        self.assertRaises(CertInvalidError, composite_db.delete, '')

        # Insert and delete the same certs before commit
        inserted = insert_test_certs(composite_db, TEST_CERTS_1)
        deleted = delete_test_certs(composite_db, TEST_CERTS_1)
        # transaction should be clear and files should not exist
        self.assertFalse(real_db._to_delete)
        self.assertFalse(real_db2._to_delete)
        self.assertFalse(real_db._to_insert)
        self.assertFalse(real_db2._to_insert)
        for cert in inserted:
            block_path = real_db._get_block_path(cert)
            block_path2 = real_db2._get_block_path(cert)
            assert not os.path.exists(os.path.join(block_path, cert))
            assert not os.path.exists(os.path.join(block_path2, cert))

        # Delete and insert the same certs before commit
        deleted = delete_test_certs(composite_db, TEST_CERTS_1)
        inserted = insert_test_certs(composite_db, TEST_CERTS_1)
        # transaction should contain deleted and inserted certificates
        self.assertTrue(real_db._to_delete)
        self.assertTrue(real_db2._to_delete)
        self.assertTrue(real_db._to_insert)
        self.assertTrue(real_db2._to_insert)
        for certs in real_db._to_delete.values():
            assert certs.issubset(set(deleted))
        for certs in real_db2._to_delete.values():
            assert certs.issubset(set(deleted))
        for certs in real_db._to_insert.values():
            assert certs.issubset(set(inserted))
        for certs in real_db2._to_insert.values():
            assert certs.issubset(set(inserted))
        # and files should exist
        for cert in inserted:
            block_path = real_db._get_block_path(cert)
            block_path2 = real_db2._get_block_path(cert)
            assert os.path.exists(os.path.join(block_path, cert))
            assert os.path.exists(os.path.join(block_path2, cert))
        # now commit and check that files were persisted
        ins, dlt = composite_db.commit()
        # the certs should be only inserted
        self.assertEqual(ins, len(inserted))
        self.assertEqual(dlt, 0)
        self.assertFalse(real_db._to_delete)
        self.assertFalse(real_db2._to_delete)
        self.assertFalse(real_db._to_insert)
        self.assertFalse(real_db2._to_insert)

        # Delete inserted certs, commit and check that they were deleted
        assert composite_db.exists_all(inserted)
        del_cert = inserted.pop()
        composite_db.delete(del_cert)
        assert not real_db.exists(del_cert)
        assert not real_db2.exists(del_cert)
        composite_db.commit()
        assert not real_db.exists(del_cert)
        assert not real_db2.exists(del_cert)
        for cert in inserted:
            composite_db.delete(cert)
        ins, dlt = composite_db.commit()
        self.assertEqual(ins, 0)
        self.assertEqual(dlt, len(inserted))
        # storage should be empty
        self.assertFalse(os.listdir(real_db.storage).remove(real_db.CONF_FILENAME))
        self.assertFalse(os.listdir(real_db2.storage).remove(real_db2.CONF_FILENAME))

        # Delete the same cert multiple times should not have effect
        self.assertFalse(real_db._to_delete)
        self.assertFalse(real_db2._to_delete)
        composite_db.delete('validcert')
        blocks_to_delete = {**real_db._to_delete}
        blocks_to_delete2 = {**real_db2._to_delete}
        self.assertTrue(real_db._to_delete)
        self.assertTrue(real_db2._to_delete)
        composite_db.delete('validcert')
        self.assertTrue(real_db._to_delete)
        self.assertTrue(real_db2._to_delete)
        self.assertEqual(blocks_to_delete, real_db._to_delete)
        self.assertEqual(blocks_to_delete2, real_db2._to_delete)

    def test_commit(self):
        """
        Test implementation of CompositeCertDB method COMMIT
        """
        real_db = CertFileDB(self.TEST_STORAGE_1)
        real_db2 = CertFileDB(self.TEST_STORAGE_2)
        composite_db = CompositeCertDB()
        composite_db.register(real_db)
        composite_db.register(real_db2)
        # Test commit without inserts
        ins, dlt = composite_db.commit()
        self.assertEqual(ins, 0)
        self.assertEqual(dlt, 0)

        # Insert some certificates and check correct number of committed certs
        inserted = insert_test_certs(composite_db, TEST_CERTS_1)
        ins, dlt = composite_db.commit()
        self.assertEqual(ins, len(inserted))
        self.assertEqual(dlt, 0)
        # transaction should be empty and certs should be compressed in zip files
        self.assertFalse(real_db._to_insert)
        self.assertFalse(real_db2._to_insert)
        for cert in inserted:
            block_path = real_db._get_block_path(cert)
            block_path2 = real_db2._get_block_path(cert)
            archive_path = real_db._get_block_archive(cert)
            archive_path2 = real_db2._get_block_archive(cert)
            assert not os.path.exists(os.path.join(block_path, cert))
            assert not os.path.exists(os.path.join(block_path2, cert))
            assert os.path.exists(archive_path)
            assert os.path.exists(archive_path2)

        # Insert already persisted certs and some others and commit
        inserted_again = insert_test_certs(composite_db, TEST_CERTS_1)
        inserted_new = insert_test_certs(composite_db, TEST_CERTS_2)
        ins, dlt = composite_db.commit()
        # only the other certs should be committed
        self.assertEqual(ins, len(inserted_new))
        self.assertEqual(dlt, 0)
        # and the same ones should NOT
        for cert in inserted_again:
            block_path = real_db._get_block_path(cert)
            block_path2 = real_db2._get_block_path(cert)
            assert not os.path.exists(os.path.join(block_path, cert))
            assert not os.path.exists(os.path.join(block_path2, cert))

        # Delete and insert the same not yet persisted cert and commit
        valid_cert = ['valid_cert', 'validvalidvalidvalidvalid']
        composite_db.delete(valid_cert[0])
        composite_db.insert(*valid_cert)
        composite_db.commit()
        # check that cert is persisted
        block_path = real_db._get_block_path(valid_cert[0])
        block_path2 = real_db2._get_block_path(valid_cert[0])
        archive_path = real_db._get_block_archive(valid_cert[0])
        archive_path2 = real_db2._get_block_archive(valid_cert[0])
        assert composite_db.exists(valid_cert[0])
        assert not os.path.exists(os.path.join(block_path, valid_cert[0]))
        assert not os.path.exists(os.path.join(block_path2, valid_cert[0]))
        assert os.path.exists(archive_path)
        assert os.path.exists(archive_path2)

        # Delete and insert the same already persisted cert and commit
        valid_cert = ['valid_cert', 'validvalidvalidvalidvalid_new']
        composite_db.delete(valid_cert[0])
        composite_db.insert(*valid_cert)
        composite_db.commit()
        # check that the cert was replaced
        assert composite_db.exists(valid_cert[0])
        self.assertEqual(real_db.get(valid_cert[0]), valid_cert[1])
        self.assertEqual(real_db2.get(valid_cert[0]), valid_cert[1])

    def test_rollback(self):
        """Test implementation of CompositeCertDB method ROLLBACK"""
        real_db = CertFileDB(self.TEST_STORAGE_1)
        real_db2 = CertFileDB(self.TEST_STORAGE_2)
        composite_db = CompositeCertDB()
        composite_db.register(real_db)
        composite_db.register(real_db2)
        # Test rollback without inserts
        composite_db.rollback()

        # Insert some certificates, rollback and check that blocks are deleted
        inserted = insert_test_certs(composite_db, TEST_CERTS_1)
        composite_db.rollback()
        self.assertFalse(real_db._to_insert)
        self.assertFalse(real_db2._to_insert)
        for cert in inserted:
            block_path = real_db._get_block_path(cert)
            block_path2 = real_db2._get_block_path(cert)
            assert not os.path.exists(os.path.join(block_path, cert))
            assert not os.path.exists(os.path.join(block_path2, cert))

        # Commit some certs, insert other certs and rollback
        committed = commit_test_certs(composite_db, TEST_CERTS_1)
        inserted = insert_test_certs(composite_db, TEST_CERTS_2)
        composite_db.rollback()
        # Transaction should be empty
        self.assertFalse(real_db._to_insert)
        self.assertFalse(real_db2._to_insert)
        # Commited certs should be compressed in zip files
        for cert in committed:
            block_path = real_db._get_block_path(cert)
            block_path2 = real_db2._get_block_path(cert)
            archive_path = real_db._get_block_archive(cert)
            archive_path2 = real_db2._get_block_archive(cert)
            assert not os.path.exists(os.path.join(block_path, cert))
            assert not os.path.exists(os.path.join(block_path2, cert))
            assert os.path.exists(archive_path)
            assert os.path.exists(archive_path2)
        # Rollbacked certs files should not exists
        for cert in inserted:
            block_path = real_db._get_block_path(cert)
            block_path2 = real_db2._get_block_path(cert)
            archive_path = real_db._get_block_archive(cert)
            archive_path2 = real_db2._get_block_archive(cert)
            assert not os.path.exists(os.path.join(block_path, cert))
            assert not os.path.exists(os.path.join(block_path2, cert))
            assert not os.path.exists(archive_path)
            assert not os.path.exists(archive_path2)

        # Check rollback of delete method
        deleted = delete_test_certs(composite_db, TEST_CERTS_1)
        self.assertTrue(real_db._to_delete)
        self.assertTrue(real_db2._to_delete)
        for cert in deleted:
            assert not composite_db.exists(cert)
            assert not real_db.exists(cert)
            assert not real_db2.exists(cert)
        composite_db.rollback()
        self.assertFalse(real_db._to_delete)
        self.assertFalse(real_db2._to_delete)
        # All deleted certs should still exist
        assert composite_db.exists_all(deleted)
        assert real_db.exists_all(deleted)
        assert real_db2.exists_all(deleted)


if __name__ == '__main__':
    unittest.main()

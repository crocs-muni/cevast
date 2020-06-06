"""
This module contains white-box unit tests of CertDB package
"""
import unittest
import os
import shutil
import subprocess
import toml
import time
from cevast.certdb import CertDB, CertFileDB, CertFileDBReadOnly, CertNotAvailableError, CertInvalidError
from cevast.utils import make_PEM_filename

# Helper functions
TEST_DATA_PATH = 'tests/data/'
TEST_CERTS_1 = TEST_DATA_PATH + 'test_certs_1.csv'
TEST_CERTS_2 = TEST_DATA_PATH + 'test_certs_2.csv'


def insert_test_certs(database: CertDB, certs_file: str) -> list:
    """
    Insert certificates from certs_file to database
    Return list of committed certificates.
    """
    certs = []
    with open(certs_file) as r_file:
        for line in r_file:
            els = [e.strip() for e in line.split(',')]
            database.insert(els[0], els[1])
            certs.append(els[0])

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


# pylint: disable=W0212
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
        self.assertEqual(cfg['PARAMETERS']['storage'], os.path.abspath(self.TEST_STORAGE))
        self.assertEqual(cfg['PARAMETERS']['structure_level'], 5)
        self.assertEqual(cfg['PARAMETERS']['cert_format'], 'DES')
        self.assertEqual(cfg['INFO']['description'], 'Testing DB')
        self.assertEqual(cfg['INFO']['owner'], 'unittest')
        self.assertEqual(cfg['INFO']['number_of_certificates'], 0)
        assert 'compression_method' in cfg['PARAMETERS']
        assert 'last_commit' in cfg['INFO']
        assert 'HISTORY' in cfg
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
        CertFileDBReadOnly.setup(self.TEST_STORAGE)
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
            fake_target_dir = 'tests/fake_export'

            os.mkdir(fake_target_dir)
            subprocess.call(['chmod', '-w', fake_target_dir])
            self.assertRaises(PermissionError, db.export, valid_cert_id, fake_target_dir)
            subprocess.call(['chmod', '+w', fake_target_dir])
            os.rmdir(fake_target_dir)

        CertFileDBReadOnly.setup(self.TEST_STORAGE)
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
                expected = '{}/{}.pem'.format(target_dir, cert_id)
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
        CertFileDBReadOnly.setup(self.TEST_STORAGE)
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
        CertFileDB.setup(self.TEST_STORAGE)
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
        # TODO Check speed improvement using cache - on large number of certs
        #t0 = time.clock()
        #for cert in certs:
            #db_ronly.exists(cert)
        #t1 = time.clock()
        #for cert in certs:
            #db_ronly.exists(cert)
        #t2 = time.clock()
        #self.assertGreater(t1 - t0, t2 - t1)


class TestCertFileDB(unittest.TestCase):
    """Unit test class of CertFileDB class"""

    TEST_STORAGE = 'tests/test_storage'

    def tearDown(self):
        # Clear test storage
        shutil.rmtree(self.TEST_STORAGE, ignore_errors=True)

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
        CertFileDB.setup(self.TEST_STORAGE)
        db = CertFileDB(self.TEST_STORAGE)
        fake_cert_id = 'fakecertid'
        # Insert and commit some certificates and retrieve them back
        commit_test_certs(db, TEST_CERTS_1)
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
        # Test fake certificate that doesn't exist
        self.assertRaises(CertNotAvailableError, db.get, fake_cert_id)

    def test_export(self):
        """
        Test implementation of CertDB method EXPORT
        """

        def test_permission(db, valid_cert_id):
            fake_target_dir = 'tests/fake_export'

            os.mkdir(fake_target_dir)
            subprocess.call(['chmod', '-w', fake_target_dir])
            self.assertRaises(PermissionError, db.export, valid_cert_id, fake_target_dir)
            subprocess.call(['chmod', '+w', fake_target_dir])
            os.rmdir(fake_target_dir)

        CertFileDB.setup(self.TEST_STORAGE)
        db = CertFileDB(self.TEST_STORAGE)
        target_dir = self.TEST_STORAGE + '/export'
        os.mkdir(target_dir)
        fake_cert_id = 'fakecertid'
        # Insert and commit some certificates and export them
        commit_test_certs(db, TEST_CERTS_1)
        with open(TEST_CERTS_1) as r_file:
            for line in r_file:
                cert_id, cert = line.split(',')
                expected = '{}/{}.pem'.format(target_dir, cert_id)
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
                expected = '{}/{}.pem'.format(target_dir, cert_id)
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
        # Test fake certificate that doesn't exist
        self.assertRaises(CertNotAvailableError, db.export, fake_cert_id, target_dir)

    def test_exists(self):
        """
        Test implementation of CertDB method EXISTS
        """
        CertFileDB.setup(self.TEST_STORAGE)
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
        # Test fake certificate that doesn't exist
        committed.append(fake_cert)
        assert not db.exists(fake_cert)
        assert not db.exists_all(committed)

    def test_insert(self):
        """
        Test implementation of CertDB method INSERT
        """
        CertFileDB.setup(self.TEST_STORAGE)
        db = CertFileDB(self.TEST_STORAGE)
        # Insert some invalid certificates
        self.assertRaises(CertInvalidError, db.insert, None, None)
        self.assertRaises(CertInvalidError, db.insert, '', '')
        self.assertRaises(CertInvalidError, db.insert, '', 'valid')
        self.assertRaises(CertInvalidError, db.insert, 'valid', None)
        # Insert some valid certificates
        insert_test_certs(db, TEST_CERTS_1)
        blocks = {**db._to_insert}
        # Transaction should contain certificates from open transcation and folders should exists
        self.assertTrue(db._to_insert)
        for block in db._to_insert:
            assert os.path.exists(db._get_block_path(block))
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
        # TODO

    def test_rollback(self):
        """
        Test implementation of CertDB method ROLLBACK
        """
        CertFileDB.setup(self.TEST_STORAGE)
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
            assert not os.path.exists(os.path.join(block_path, make_PEM_filename(cert)))
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
            assert not os.path.exists(db._get_block_path(cert))
            assert os.path.exists(db._get_block_path(cert) + '.zip')
        # Rollbacked certs files should not exists
        for cert in inserted:
            block_path = db._get_block_path(cert)
            assert not os.path.exists(os.path.join(block_path, make_PEM_filename(cert)))
        # Check rollback of delete method
        deleted = delete_test_certs(db, TEST_CERTS_1)
        self.assertTrue(db._to_delete)
        assert db.exists_all(deleted)
        db.rollback()
        self.assertFalse(db._to_delete)
        # All deleted certs should still exist
        assert db.exists_all(deleted)

    def test_commit(self):
        """
        Test implementation of CertDB method COMMIT
        """
        CertFileDB.setup(self.TEST_STORAGE)
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
            assert os.path.exists(os.path.join(block_path, make_PEM_filename(cert)))
        # Check correct number of committed certs
        ins, dlt = db.commit()
        self.assertEqual(ins, len(inserted))
        self.assertEqual(dlt, 0)
        # Transaction should be empty and certs should be compressed in zip files
        self.assertFalse(db._to_insert)
        for cert in inserted:
            assert not os.path.exists(db._get_block_path(cert))
            assert os.path.exists(db._get_block_path(cert) + '.zip')
        # Insert already persisted certs and some others
        inserted_again = insert_test_certs(db, TEST_CERTS_1)
        inserted_new = insert_test_certs(db, TEST_CERTS_2)
        ins, dlt = db.commit()
        # Only the other certs should be committed
        self.assertEqual(ins, len(inserted_new))
        self.assertEqual(dlt, 0)
        # and the same ones should be deleted from transaction
        for cert in inserted_again:
            block_path = db._get_block_path(cert)
            assert not os.path.exists(os.path.join(block_path, make_PEM_filename(cert)))
        # TODO delete
 
    def test_parallel_transactions(self):
        pass

    def test_config_info_maintain(self):
        pass


if __name__ == '__main__':
    unittest.main()

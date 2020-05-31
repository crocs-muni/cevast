"""
This module contains white-box unit tests of CertDB package
"""
import unittest
import os
import shutil
import subprocess
import toml
from cevast.certdb import CertDB, CertFileDB, CertFileDBReadOnly, CertNotAvailableError, CertInvalidError


# Helper functions
TEST_DATA_PATH = 'tests/data/'
TEST_CERTS_1 = TEST_DATA_PATH + 'test_certs_1.csv'
TEST_CERTS_2 = TEST_DATA_PATH + 'test_certs_2.csv'


def insert_test_certs(database: CertDB, certs_file: str) -> list:
    certs = []
    with open(certs_file) as r_file:
        for line in r_file:
            els = [e.strip() for e in line.split(',')]
            database.insert(els[0], els[1])
            certs.append(els[0])

    return certs


def commit_test_certs(database: CertDB, certs_file: str) -> list:
    certs = insert_test_certs(database, certs_file)
    database.commit()
    return certs


# pylint: disable=W0212
class TestCertFileDBReadOnly(unittest.TestCase):
    TEST_STORAGE = 'tests/test_storage'

    def tearDown(self):
        # Clear test storage
        shutil.rmtree(self.TEST_STORAGE, ignore_errors=True)

    def test_init(self):
        """
        Test of CertFileDBReadOnly initialization
        """
        self.assertRaises(ValueError, CertFileDBReadOnly, self.TEST_STORAGE)
        CertFileDB.setup(self.TEST_STORAGE)
        # Storage should be now properly initialized
        CertFileDBReadOnly(self.TEST_STORAGE)

    def test_exists(self):
        """
        Test implementation of CertDB method insert
        """
        CertFileDB.setup(self.TEST_STORAGE)
        db = CertFileDB(self.TEST_STORAGE)
        certs = []
        fake_cert = 'fakecertid'
        # Insert and commit some certificates and check if exists
        for cert in commit_test_certs(db, TEST_CERTS_1):
            assert db.exists(cert)
            certs.append(cert)
        assert db.exists_all(certs)
        # Only insert other certificates and check if exists
        for cert in insert_test_certs(db, TEST_CERTS_2):
            assert db.exists(cert)
            certs.append(cert)
        assert db.exists_all(certs)
        # Test fake certificate that doesn't exist
        certs.append(fake_cert)
        assert not db.exists(fake_cert)
        assert not db.exists_all(certs)

    def test_get(self):
        """
        Test implementation of CertDB method get
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
        Test implementation of CertDB method export
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


class TestCertFileDB(unittest.TestCase):
    TEST_STORAGE = 'tests/test_storage'

    def tearDown(self):
        # Clear test storage
        shutil.rmtree(self.TEST_STORAGE, ignore_errors=True)

    def test_init(self):
        """
        Test of CertFileDB initialization
        """
        assert not os.path.exists(self.TEST_STORAGE)
        self.assertRaises(ValueError, CertFileDBReadOnly, self.TEST_STORAGE)
        CertFileDB.setup(self.TEST_STORAGE)
        # Storage should be now properly initialized
        assert os.path.exists(self.TEST_STORAGE)
        CertFileDB(self.TEST_STORAGE)

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
        targets = tuple(db._to_insert)
        # Transaction should contain certificates from open transcation and folders should exists
        self.assertTrue(db._to_insert)
        for trg in targets:
            assert os.path.exists(trg)
        # Insert different certificates under the same IDs
        certs = {}
        with open(TEST_CERTS_1) as r_file:
            for line in r_file:
                els = [e.strip() for e in line.split(',')]
                db.insert(els[0], els[1] + '_open')
                certs[els[0]] = els[1]
        # IDs should be same and certificates should not be changed
        self.assertTrue(targets == tuple(db._to_insert))
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
        self.assertTrue(targets == tuple(db._to_insert))
        db.commit()
        self.assertFalse(db._to_insert)
        for k, v in certs.items():
            self.assertTrue(db.get(k) == v)

    def test_commit(self):
        """
        Test implementation of CertDB method COMMIT
        """
        CertFileDB.setup(self.TEST_STORAGE)
        db = CertFileDB(self.TEST_STORAGE)
        # Test commit without inserts
        db.commit()
        self.assertFalse(db._to_insert)
        # Insert some certificates and check commit
        insert_test_certs(db, TEST_CERTS_1)
        targets = tuple(db._to_insert)
        # Transaction should contain certificates from open transaction and folders should exists
        self.assertTrue(db._to_insert)
        for trg in targets:
            assert os.path.exists(trg)
        db.commit()
        # Transaction should be empty and certs should be compressed in zip files
        self.assertFalse(db._to_insert)
        for trg in targets:
            assert not os.path.exists(trg)
            assert os.path.exists(trg + '.zip')

    def test_rollback(self):
        """
        Test implementation of CertDB method ROLLBACK
        """
        CertFileDB.setup(self.TEST_STORAGE)
        db = CertFileDB(self.TEST_STORAGE)
        # Test rollback without inserts
        db.rollback()
        self.assertFalse(db._to_insert)
        # Insert some certificates and check rollback
        insert_test_certs(db, TEST_CERTS_1)
        targets = tuple(db._to_insert)
        # Transaction should contain certificates from open transcation and folders should exist
        self.assertTrue(db._to_insert)
        for trg in targets:
            assert os.path.exists(trg)
        # Commit actual certs, insert other certs and rollback
        db.commit()
        insert_test_certs(db, TEST_CERTS_2)
        rollback_targets = tuple(db._to_insert)
        db.rollback()
        # Transaction should be empty
        self.assertFalse(db._to_insert)
        # Commited certs should be compressed in zip files
        for trg in targets:
            assert not os.path.exists(trg)
            assert os.path.exists(trg + '.zip')
        # Rollbacked certs files should not exists
        for trg in rollback_targets:
            assert not os.path.exists(trg)
            assert not os.path.exists(trg + '.zip')

    def test_delete(self):
        """
        Test implementation of CertDB method DELETE
        """
        # TODO

    def test_cache(self):
        """
        Test implementation of CertDB certificate existance cache
        """
        CertFileDB.setup(self.TEST_STORAGE)
        db = CertFileDB(self.TEST_STORAGE)
        inserted = insert_test_certs(db, TEST_CERTS_1)
        self.assertEqual(db._cache, set(inserted))
        # TODO

    def test_setup(self):
        """
        Test implementation of CertFileDB setup method
        """
        # Check wrong paramaters
        self.assertRaises(ValueError, CertFileDB.setup, self.TEST_STORAGE, 'ass')
        # Setup and check DB
        CertFileDB.setup(self.TEST_STORAGE, 5, 'DES', 'Testing DB', 'unittest')
        assert os.path.exists(self.TEST_STORAGE)
        cfg = toml.load(os.path.join(self.TEST_STORAGE, CertFileDB._CONF_FILE))
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


if __name__ == '__main__':
    unittest.main()

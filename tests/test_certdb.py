"""
This module contains white-box unit tests of CertDB package
"""
import unittest
import os
import shutil
import subprocess
from cevast.certdb import CertDB, CertFileDB, CertFileDBReadOnly, CertNotAvailableError, CertInvalidError


"""
Helper functions
"""
TEST_DATA_PATH = 'tests/data/'
TEST_CERTS_1 = TEST_DATA_PATH + 'test_certs_1.csv'
TEST_CERTS_2 = TEST_DATA_PATH + 'test_certs_2.csv'


def insert_test_certs(db: CertDB, certs_file: str):
    with open(certs_file) as cf:
        for l in cf:
            els = [e.strip() for e in l.split(',')]
            db.insert(els[0], els[1])


def commit_test_certs(db: CertDB, certs_file: str):
    insert_test_certs(db, certs_file)
    db.commit()


class TestCertFileDBReadOnly(unittest.TestCase):
    TEST_STORAGE = 'tests/test_storage'
    TEST_STORAGE_CERTS = TEST_STORAGE + '/certs'

    def tearDown(self):
        # Clear test storage
        shutil.rmtree(self.TEST_STORAGE, ignore_errors=True)

    def test_init(self):
        """
        Test of CertFileDBReadOnly initialization
        """
        self.assertRaises(ValueError, CertFileDBReadOnly, self.TEST_STORAGE)
        os.mkdir(self.TEST_STORAGE)
        self.assertRaises(ValueError, CertFileDBReadOnly, self.TEST_STORAGE)
        os.mkdir(self.TEST_STORAGE_CERTS)
        # Storage should be now properly initialized
        CertFileDBReadOnly(self.TEST_STORAGE)

    def test_exists(self):
        """
        Test implementation of CertDB method insert
        """
        db = CertFileDB(self.TEST_STORAGE)
        certs = []
        fake_cert = 'fakecertid'
        # Insert and commit some certificates and check if exists
        commit_test_certs(db, TEST_CERTS_1)
        with open(TEST_CERTS_1) as cf:
            for l in cf:
                cert = l.split(',')[0]
                assert db.exists(cert)
                certs.append(cert)
        assert db.exists_all(certs)
        # Only insert other certificates and check if exists
        insert_test_certs(db, TEST_CERTS_2)
        with open(TEST_CERTS_2) as cf:
            for l in cf:
                cert = l.split(',')[0]
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
        db = CertFileDB(self.TEST_STORAGE)
        fake_cert_id = 'fakecertid'
        # Insert and commit some certificates and retrieve them back
        commit_test_certs(db, TEST_CERTS_1)
        with open(TEST_CERTS_1) as cf:
            for l in cf:
                cert_id, cert = l.split(',')
                self.assertEqual(db.get(cert_id), cert.strip())
        # Only insert other certificates and retrieve them back
        insert_test_certs(db, TEST_CERTS_2)
        with open(TEST_CERTS_2) as cf:
            for l in cf:
                cert_id, cert = l.split(',')
                self.assertEqual(db.get(cert_id), cert.strip())
        # Rollback and try to retrieve them again
            db.rollback()
            cf.seek(0)
            for l in cf:
                cert_id = l.split(',')[0]
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

        db = CertFileDB(self.TEST_STORAGE)
        target_dir = self.TEST_STORAGE + '/export'
        os.mkdir(target_dir)
        fake_cert_id = 'fakecertid'
        # Insert and commit some certificates and export them
        commit_test_certs(db, TEST_CERTS_1)
        with open(TEST_CERTS_1) as cf:
            for l in cf:
                cert_id, cert = l.split(',')
                expected = '{}/{}.pem'.format(target_dir, cert_id)
                self.assertEqual(db.export(cert_id, target_dir), expected)
                with open(expected) as target:
                    self.assertEqual(target.read(), cert.strip())
        # Tests writing permissions for exporting from zipfile
        test_permission(db, cert_id)
        # Only insert other certificates and retrieve them back
        insert_test_certs(db, TEST_CERTS_2)
        with open(TEST_CERTS_2) as cf:
            for l in cf:
                cert_id, cert = l.split(',')
                expected = '{}/{}.pem'.format(target_dir, cert_id)
                self.assertEqual(db.export(cert_id, target_dir), expected)
                with open(expected) as target:
                    self.assertEqual(target.read(), cert.strip())
        # Tests writing permissions for exporting from transaction
            test_permission(db, cert_id)
        # Rollback and try to retrieve them again
            db.rollback()
            cf.seek(0)
            for l in cf:
                cert_id = l.split(',')[0]
                self.assertRaises(CertNotAvailableError, db.export, cert_id, target_dir)
        # Test fake certificate that doesn't exist
        self.assertRaises(CertNotAvailableError, db.export, fake_cert_id, target_dir)


class TestCertFileDB(unittest.TestCase):
    TEST_STORAGE = 'tests/test_storage'
    TEST_STORAGE_CERTS = TEST_STORAGE + '/certs'

    def tearDown(self):
        # Clear test storage
        shutil.rmtree(self.TEST_STORAGE, ignore_errors=True)

    def test_init(self):
        """
        Test of CertFileDB initialization
        """
        assert not os.path.exists(self.TEST_STORAGE)
        CertFileDB(self.TEST_STORAGE)
        assert os.path.exists(self.TEST_STORAGE)
        assert os.path.exists(self.TEST_STORAGE + '/certs')

    def test_insert(self):
        """
        Test implementation of CertDB method insert
        """
        db = CertFileDB(self.TEST_STORAGE)
        # Insert some invalid certificates
        self.assertRaises(CertInvalidError, db.insert, None, None)
        self.assertRaises(CertInvalidError, db.insert, '', '')
        self.assertRaises(CertInvalidError, db.insert, '', 'valid')
        self.assertRaises(CertInvalidError, db.insert, 'valid', None)
        # Insert some valid certificates
        insert_test_certs(db, TEST_CERTS_1)
        targets = tuple(db._transaction)
        # Transaction should contain certificates from open transcation and folders should exists
        self.assertTrue(db._transaction)
        for t in targets:
            assert os.path.exists(t)
        # Insert different certificates under the same IDs
        certs = {}
        with open(TEST_CERTS_1) as cf:
            for l in cf:
                els = [e.strip() for e in l.split(',')]
                db.insert(els[0], els[1] + '_open')
                certs[els[0]] = els[1]
        # IDs should be same and certificates should not be changed
        self.assertTrue(targets == tuple(db._transaction))
        for k, v in certs.items():
            self.assertTrue(db.get(k) == v)
        # Commit transaction and commit different certificates under the same IDs
        db.commit()
        self.assertFalse(db._transaction)
        certs = {}
        with open(TEST_CERTS_1) as cf:
            for l in cf:
                els = [e.strip() for e in l.split(',')]
                db.insert(els[0], els[1] + '_commit')
                certs[els[0]] = els[1]
        # IDs should be same and persisted certificates should not be changed
        self.assertTrue(targets == tuple(db._transaction))
        db.commit()
        self.assertFalse(db._transaction)
        for k, v in certs.items():
            self.assertTrue(db.get(k) == v)

    def test_commit(self):
        """
        Test implementation of CertDB method commit
        """
        db = CertFileDB(self.TEST_STORAGE)
        # Test commit without inserts
        db.commit()
        self.assertFalse(db._transaction)
        # Insert some certificates and check commit
        insert_test_certs(db, TEST_CERTS_1)
        targets = tuple(db._transaction)
        # Transaction should contain certificates from open transcation and folders should exists
        self.assertTrue(db._transaction)
        for t in targets:
            assert os.path.exists(t)
        db.commit()
        # Transaction should be empty and certs should be compressed in zip files
        self.assertFalse(db._transaction)
        for t in targets:
            assert not os.path.exists(t)
            assert os.path.exists(t + '.zip')

    def test_rollback(self):
        """
        Test implementation of CertDB method rollback
        """
        db = CertFileDB(self.TEST_STORAGE)
        # Test rollback without inserts
        db.rollback()
        self.assertFalse(db._transaction)
        # Insert some certificates and check rollback
        insert_test_certs(db, TEST_CERTS_1)
        targets = tuple(db._transaction)
        # Transaction should contain certificates from open transcation and folders should exist
        self.assertTrue(db._transaction)
        for t in targets:
            assert os.path.exists(t)
        # Commit actual certs, insert other certs and rollback
        db.commit()
        insert_test_certs(db, TEST_CERTS_2)
        rollback_targets = tuple(db._transaction)
        db.rollback()
        # Transaction should be empty
        self.assertFalse(db._transaction)
        # Commited certs should be compressed in zip files
        for t in targets:
            assert not os.path.exists(t)
            assert os.path.exists(t + '.zip')
        # Rollbacked certs files should not exists
        for t in rollback_targets:
            assert not os.path.exists(t)
            assert not os.path.exists(t + '.zip')


if __name__ == '__main__':
    unittest.main()

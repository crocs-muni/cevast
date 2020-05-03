"""
This module contains white-box unit tests of CertDB package
"""
import unittest
import os
import shutil
from cevast.certdb import CertDB, CertFileDB, CertFileDBReadOnly


"""
Helper functions
"""
TEST_DATA_PATH = 'tests/data/'
TEST_CERTS_1 = TEST_DATA_PATH + 'test_certs_1.csv'
TEST_CERTS_2 = TEST_DATA_PATH + 'test_certs_2.csv'


def insert_test_certs(db: CertDB, certs_file: str):
    with open(certs_file) as cf:
        for l in cf:
            db.insert(*l.split(','))


def commit_test_certs(db: CertDB, certs_file: str):
    insert_test_certs(db, certs_file)
    db.commit()


class TestCertFileDBReadOnly(unittest.TestCase):
    TEST_STORAGE = 'tests/test_storage'
    TEST_STORAGE_CERTS = TEST_STORAGE + '/certs'

    def test_init(self):
        """
        Test of CertFileDBReadOnly initialization
        """
        self.assertRaises(ValueError, CertFileDBReadOnly, self.TEST_STORAGE)
        os.mkdir(self.TEST_STORAGE)
        self.assertRaises(ValueError, CertFileDBReadOnly, self.TEST_STORAGE)
        os.mkdir(self.TEST_STORAGE_CERTS)
        # Storage should be now properly initialized
        try:
            CertFileDBReadOnly(self.TEST_STORAGE)
        except ValueError:
            assert False
        # Clear test storage
        shutil.rmtree(self.TEST_STORAGE)

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
        # Clear test storage
        shutil.rmtree(self.TEST_STORAGE)

    def test_get(self):
        """
        Test implementation of CertDB method commit
        """
        pass

    def test_invalid_cert_exception(self):
        """
        Test implementation of CertDB method commit
        """
        pass

    def test_cert_not_available_exception(self):
        """
        Test implementation of CertDB method commit
        """
        pass


class TestCertFileDB(unittest.TestCase):
    TEST_STORAGE = 'tests/test_storage'
    TEST_STORAGE_CERTS = TEST_STORAGE + '/certs'

    def test_init(self):
        """
        Test of CertFileDB initialization
        """
        assert not os.path.exists(self.TEST_STORAGE)
        CertFileDB(self.TEST_STORAGE)
        assert os.path.exists(self.TEST_STORAGE)
        assert os.path.exists(self.TEST_STORAGE + '/certs')
        self.__cleanup()

    def test_insert(self):
        """
        Test implementation of CertDB method insert
        """
        # Test some invalid certs
        pass

    def test_commit(self):
        """
        Test implementation of CertDB method commit
        """
        db = CertFileDB(self.TEST_STORAGE)
        # Test commit without inserts
        db.commit()
        assert not db._journal
        # Insert some certificates and check commit
        insert_test_certs(db, TEST_CERTS_1)
        targets = tuple(db._journal)
        # Journal should contain certificates from open transcation and folders should exists
        assert db._journal
        for t in targets:
            assert os.path.exists(t)
        db.commit()
        # Journal should be empty and certs should be compressed in zip files
        assert not db._journal
        for t in targets:
            assert not os.path.exists(t)
            assert os.path.exists(t + '.zip')
        # Clear test storage
        self.__cleanup()

    def test_rollback(self):
        """
        Test implementation of CertDB method rollback
        """
        db = CertFileDB(self.TEST_STORAGE)
        # Test rollback without inserts
        db.rollback()
        assert not db._journal
        # Insert some certificates and check rollback
        insert_test_certs(db, TEST_CERTS_1)
        targets = tuple(db._journal)
        # Journal should contain certificates from open transcation and folders should exist
        assert db._journal
        for t in targets:
            assert os.path.exists(t)
        # Commit actual certs, insert other certs and rollback
        db.commit()
        insert_test_certs(db, TEST_CERTS_2)
        rollback_targets = tuple(db._journal)
        db.rollback()
        # Journal should be empty
        assert not db._journal
        # Commited certs should be compressed in zip files
        for t in targets:
            assert not os.path.exists(t)
            assert os.path.exists(t + '.zip')
        # Rollbacked certs files should not exists
        for t in rollback_targets:
            assert not os.path.exists(t)
            assert not os.path.exists(t + '.zip')
        # Clear test storage
        self.__cleanup()

    def __cleanup(self):
        shutil.rmtree(self.TEST_STORAGE)


if __name__ == '__main__':
    unittest.main()

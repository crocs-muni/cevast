"""
This module contains unit tests of Dataset module
"""

import os
import shutil
import unittest
from cevast.dataset.dataset import (DatasetPath, DatasetState, DatasetType, DatasetInvalidError, DatasetRepository)


def create_file(name: str):
    """Create file with given name."""
    os.makedirs(os.path.dirname(name), exist_ok=True)
    with open(name, 'w') as f:
        f.write("adadadadadasdadadadasda")


class TestDatasetPath(unittest.TestCase):
    """Unit test class of DatasetPath class"""

    TEST_REPO = os.path.join('tests', 'test_repository')

    def setUp(self):
        # Create test repository
        os.makedirs(self.TEST_REPO)

    def tearDown(self):
        # Clear test repository
        shutil.rmtree(self.TEST_REPO, ignore_errors=True)

    def test_init(self):
        """Test of DatasetPath class instantiation."""
        # Test init with wrong parameters
        self.assertRaises(DatasetInvalidError, DatasetPath, self.TEST_REPO, DatasetState.PARSED, '')
        self.assertRaises(DatasetInvalidError, DatasetPath, self.TEST_REPO, 5, '')
        self.assertRaises(FileNotFoundError, DatasetPath, self.TEST_REPO + 'invalid', DatasetType.RAPID, '')
        # Create DatasetPath instance
        DatasetPath(self.TEST_REPO, DatasetType.RAPID, '2020-06-12')

        # Test init with STRING type paramater
        DatasetPath(self.TEST_REPO, "RAPID", '2020-06-12')
        self.assertRaises(DatasetInvalidError, DatasetPath, self.TEST_REPO, "UNKNOWN", '2020-06-12')

    def test_get(self):
        """Test implementation of DatasetPath method GET."""
        dp_rapid = DatasetPath(self.TEST_REPO, DatasetType.RAPID, '2020-06-12')
        dp_censys = DatasetPath(self.TEST_REPO, DatasetType.CENSYS, '2020-06-12')
        # Test GET with wrong state parameter
        self.assertRaises(DatasetInvalidError, dp_rapid.get, 2)

        # Test GET with correct state paramater
        path = dp_rapid.get(DatasetState.PARSED, False)
        path2 = dp_rapid.get(DatasetState.VALIDATED, False)
        path3 = dp_censys.get(DatasetState.VALIDATED, False)
        assert not os.path.exists(path)
        self.assertNotEqual(path, path2)
        self.assertNotEqual(path2, path3)
        # GET should return /../repository/RAPID/PARSED
        self.assertEqual(path, os.path.join(os.path.abspath(self.TEST_REPO), DatasetType.RAPID.name, DatasetState.PARSED.name))

        # Test GET with STRING state paramater
        path = dp_rapid.get("PARSED", False)
        assert not os.path.exists(path)
        self.assertEqual(path, os.path.join(os.path.abspath(self.TEST_REPO), DatasetType.RAPID.name, DatasetState.PARSED.name))

        # Test check_if_exists paramater
        assert not os.path.exists(path)
        self.assertEqual(dp_rapid.get(DatasetState.PARSED, True), None)
        os.makedirs(path)
        assert os.path.exists(path)
        self.assertEqual(dp_rapid.get(DatasetState.PARSED, True), os.path.join(os.path.abspath(self.TEST_REPO), DatasetType.RAPID.name, DatasetState.PARSED.name))

    def test_get_full(self):
        """Test implementation of DatasetPath method GET_FULL."""
        dp_rapid = DatasetPath(self.TEST_REPO, DatasetType.RAPID, '2020-06-12')
        dp_censys = DatasetPath(self.TEST_REPO, DatasetType.CENSYS, '2020-06-30')
        # Test GET_FULL with wrong state parameter
        self.assertRaises(DatasetInvalidError, dp_rapid.get_full, 2)

        # Test GET_FULL with correct state paramater
        path = dp_rapid.get_full(DatasetState.PARSED, '', False)
        path2 = dp_rapid.get_full(DatasetState.VALIDATED, '', False)
        path3 = dp_censys.get_full(DatasetState.VALIDATED, '', False)
        assert not os.path.exists(path)
        self.assertNotEqual(path, path2)
        self.assertNotEqual(path2, path3)
        # GET_FULL should return /../repository/RAPID/PARSED/2020-06-12.gz
        self.assertEqual(path, os.path.join(os.path.abspath(self.TEST_REPO), DatasetType.RAPID.name, DatasetState.PARSED.name, '2020-06-12.' + dp_rapid.EXTENSION))
        path = dp_rapid.get_full(DatasetState.PARSED, 'suffix', False)
        # GET_FULL should return /../repository/RAPID/PARSED/2020-06-12_suffix.gz
        self.assertEqual(path, os.path.join(os.path.abspath(self.TEST_REPO), DatasetType.RAPID.name, DatasetState.PARSED.name, '2020-06-12_suffix.' + dp_rapid.EXTENSION))

        # Test GET_FULL with STRING state paramater
        path = dp_rapid.get_full("PARSED", '', False)
        assert not os.path.exists(path)
        self.assertEqual(path, os.path.join(os.path.abspath(self.TEST_REPO), DatasetType.RAPID.name, DatasetState.PARSED.name, '2020-06-12.' + dp_rapid.EXTENSION))

        # Test check_if_exists paramater
        assert not os.path.exists(path)
        self.assertEqual(dp_rapid.get_full(DatasetState.PARSED, '', True), None)
        os.makedirs(path)
        assert os.path.exists(path)
        self.assertEqual(dp_rapid.get_full(DatasetState.PARSED, '', True), os.path.join(os.path.abspath(self.TEST_REPO), DatasetType.RAPID.name, DatasetState.PARSED.name, '2020-06-12.' + dp_rapid.EXTENSION))

    def test_delete(self):
        """Test implementation of DatasetPath method DELETE."""
        # Test DELETE with not existing file
        dp_rapid = DatasetPath(self.TEST_REPO, DatasetType.RAPID, '2020-06-12')
        dp_rapid2 = DatasetPath(self.TEST_REPO, DatasetType.RAPID, '2020-06-30')
        dp_rapid.delete(DatasetState.PARSED)

        # Create some datasets
        path = dp_rapid.get(DatasetState.PARSED, False)
        create_file(os.path.join(path, '2020-06-12.gz'))
        create_file(os.path.join(path, '2020-06-30.gz'))
        create_file(os.path.join(path, '2020-06-30_suffix.gz'))
        assert os.path.exists(os.path.join(path, '2020-06-12.gz'))
        assert os.path.exists(os.path.join(path, '2020-06-30.gz'))
        assert os.path.exists(os.path.join(path, '2020-06-30_suffix.gz'))
        dp_rapid.delete(DatasetState.VALIDATED)
        # dataset should still exists
        assert os.path.exists(os.path.join(path, '2020-06-12.gz'))
        dp_rapid.delete(DatasetState.PARSED)
        # dataset should NOT exists but others yes
        assert not os.path.exists(os.path.join(path, '2020-06-12.gz'))
        assert os.path.exists(os.path.join(path, '2020-06-30.gz'))
        assert os.path.exists(os.path.join(path, '2020-06-30_suffix.gz'))
        dp_rapid2.delete(DatasetState.PARSED)
        # whole dataset state directory should be deleted
        assert not os.path.exists(os.path.join(path, '2020-06-30.gz'))
        assert not os.path.exists(os.path.join(path, '2020-06-30_suffix.gz'))
        assert not os.path.exists(path)

        # Test DELETE with STRING state paramater
        path = dp_rapid.get(DatasetState.PARSED, False)
        create_file(os.path.join(path, '2020-06-12.gz'))
        assert os.path.exists(path)
        self.assertRaises(DatasetInvalidError, dp_rapid.delete, "UNKNOWN")
        dp_rapid2.delete("PARSED")
        assert os.path.exists(path)
        dp_rapid.delete("PARSED")
        assert not os.path.exists(path)

    def test_purge(self):
        """Test implementation of DatasetPath method PURGE."""
        dp_rapid = DatasetPath(self.TEST_REPO, DatasetType.RAPID, '2020-06-12')
        dp_censys = DatasetPath(self.TEST_REPO, DatasetType.CENSYS, '2020-06-12')
        path_r = os.path.join(self.TEST_REPO, DatasetType.RAPID.name)
        path_c = os.path.join(self.TEST_REPO, DatasetType.CENSYS.name)
        # Test PURGE on empty repository
        dp_rapid.purge()
        # Create some datasets and PURGE rapid repository
        create_file(os.path.join(path_r, '2020-06-12.gz'))
        create_file(os.path.join(path_c, '2020-06-12.gz'))
        assert os.path.exists(path_r)
        assert os.path.exists(path_c)
        dp_rapid.purge()
        assert not os.path.exists(path_r)
        assert os.path.exists(path_c)
        # now purge also the other repository
        dp_censys.purge()
        assert not os.path.exists(path_r)
        assert not os.path.exists(path_c)
        assert not os.listdir(self.TEST_REPO)

    def test_exists(self):
        """Test implementation of DatasetPath method EXISTS and EXISTS_ANY."""
        dp_rapid = DatasetPath(self.TEST_REPO, DatasetType.RAPID, '2020-06-12')
        dp_censys = DatasetPath(self.TEST_REPO, DatasetType.CENSYS, '2020-06-12')
        path_r = os.path.join(self.TEST_REPO, DatasetType.RAPID.name, DatasetState.PARSED.name)
        path_c = os.path.join(self.TEST_REPO, DatasetType.CENSYS.name, DatasetState.PARSED.name)
        assert not dp_rapid.exists_any()
        assert not dp_rapid.exists_any()
        assert not dp_censys.exists(DatasetState.PARSED)
        # Create RAPID dataset and check if exists
        create_file(os.path.join(path_r, '2020-06-12.gz'))
        assert dp_rapid.exists_any()
        assert dp_rapid.exists(DatasetState.PARSED)
        assert not dp_rapid.exists(DatasetState.VALIDATED)
        assert not dp_censys.exists_any()
        # create different dataset ID that should NOT exists
        create_file(os.path.join(path_c, '2020-06-30_suffix.gz'))
        assert not dp_censys.exists_any()
        assert not dp_censys.exists(DatasetState.PARSED)
        # now create correct CENSYS dataset and check if exists
        create_file(os.path.join(path_c, '2020-06-12_suffix.gz'))
        assert dp_censys.exists_any()
        assert dp_censys.exists(DatasetState.PARSED)
        assert not dp_censys.exists(DatasetState.VALIDATED)

        # Test STRING state paramater
        assert dp_censys.exists("PARSED")
        self.assertRaises(DatasetInvalidError, dp_censys.exists, "UNKNOWN")

    def test_move(self):
        """Test implementation of DatasetPath method MOVE."""
        dp = DatasetPath(self.TEST_REPO, DatasetType.RAPID, '2020-06-12')
        path = os.path.join(self.TEST_REPO, DatasetType.RAPID.name, DatasetState.PARSED.name)
        ds = os.path.join(self.TEST_REPO, '2020-06-30_suffix.gz')
        ds_suffix_only = os.path.join(self.TEST_REPO, 'suffix.gz')
        # Test with source that doesn't exist   
        dp.move(DatasetState.PARSED, "totally_made_up")
        assert not os.path.exists(path)

        # Create dataset and move it
        create_file(ds_suffix_only)
        assert os.path.exists(ds_suffix_only)
        dp.move(DatasetState.PARSED, ds_suffix_only)
        assert os.path.exists(path)
        assert not os.path.exists(ds_suffix_only)
        assert os.path.exists(os.path.join(path, '2020-06-12_suffix.gz'))

        # Create dataset and move it without prefix
        create_file(ds)
        assert os.path.exists(ds)
        dp.move(DatasetState.PARSED, ds, False)
        assert not os.path.exists(ds)
        assert os.path.exists(os.path.join(path, '2020-06-30_suffix.gz'))

        # Test with STRING state paramater
        dp = DatasetPath(self.TEST_REPO, DatasetType.CENSYS, '2020-06-30')
        create_file(ds_suffix_only)
        self.assertRaises(DatasetInvalidError, dp.move, "UNKNOWN", ds_suffix_only)
        self.assertRaises(DatasetInvalidError, dp.move, "UNKNOWN", ds_suffix_only, False)
        dp.move("VALIDATED", ds_suffix_only)
        assert not os.path.exists(ds_suffix_only)
        assert os.path.exists(os.path.join(self.TEST_REPO, DatasetType.CENSYS.name, DatasetState.VALIDATED.name, '2020-06-30_suffix.gz'))


class TestDatasetRepository(unittest.TestCase):
    """Unit test class of DatasetRepository class"""

    TEST_REPO = os.path.join('tests', 'test_repository')

    def setUp(self):
        # Create test repository
        os.makedirs(self.TEST_REPO)

    def tearDown(self):
        # Clear test repository
        shutil.rmtree(self.TEST_REPO, ignore_errors=True)

    def test_init(self):
        """Test of DatasetRepository class instantiation."""
        # Test init with wrong repository
        self.assertRaises(FileNotFoundError, DatasetRepository, self.TEST_REPO + 'invalid')
        # Create DatasetRepository instance
        DatasetRepository(self.TEST_REPO)
        DatasetRepository(None)

    def test_get(self):
        """Test implementation of DatasetRepository method GET."""
        dp_rapid = DatasetPath(self.TEST_REPO, DatasetType.RAPID, '2020-06-12')
        dp_censys = DatasetPath(self.TEST_REPO, DatasetType.CENSYS, '2020-06-12')
        dp_censys2 = DatasetPath(self.TEST_REPO, DatasetType.CENSYS, '2020-06-30')
        repo = DatasetRepository(self.TEST_REPO)
        # Test GET on empty repository
        assert not repo.get()
        assert not repo.get(dataset_type="RAPID")
        assert not repo.get(state="PARSED")
        assert not repo.get(dataset_id="2020-06-12")
        assert not repo.get("RAPID", "PARSED", "2020-06-12")

        # Fill repository with various datasets
        create_file(os.path.join(self.TEST_REPO, 'ds1.gz'))
        dp_rapid.move(DatasetState.PARSED, os.path.join(self.TEST_REPO, 'ds1.gz'))
        create_file(os.path.join(self.TEST_REPO, 'ds2.gz'))
        dp_censys.move(DatasetState.PARSED, os.path.join(self.TEST_REPO, 'ds2.gz'))
        create_file(os.path.join(self.TEST_REPO, 'ds2_2.gz'))
        dp_censys.move(DatasetState.PARSED, os.path.join(self.TEST_REPO, 'ds2_2.gz'))
        create_file(os.path.join(self.TEST_REPO, 'ds3.gz'))
        dp_censys2.move(DatasetState.VALIDATED, os.path.join(self.TEST_REPO, 'ds3.gz'))

        # Test GET all
        get_repo = repo.get()
        self.assertEqual(get_repo, {"RAPID": {"PARSED": ('2020-06-12_ds1.gz',)},
                                    "CENSYS": {"PARSED": ('2020-06-12_ds2.gz','2020-06-12_ds2_2.gz',),
                                                "VALIDATED": ('2020-06-30_ds3.gz',)}})
        # Test GET with specific dataset ID
        self.assertEqual(get_repo, repo.get(dataset_id=""))
        self.assertEqual(get_repo, repo.get(dataset_id="2020-06-"))
        assert not repo.get(dataset_id='INVALID')
        get_repo = repo.get(dataset_id='2020-06-12')
        self.assertEqual(get_repo, {"RAPID": {"PARSED": ('2020-06-12_ds1.gz',)},
                                    "CENSYS": {"PARSED": ('2020-06-12_ds2.gz','2020-06-12_ds2_2.gz',)}})

        # Test GET with specific dataset type
        get_repo = repo.get(dataset_type=DatasetType.RAPID)
        self.assertEqual(get_repo, repo.get(dataset_type="RAPID"))
        self.assertEqual(get_repo, {"RAPID": {"PARSED": ('2020-06-12_ds1.gz',)}})

        # Test GET with specific dataset state
        assert not repo.get(state=DatasetState.ANALYZED)
        get_repo = repo.get(state=DatasetState.PARSED)
        self.assertEqual(get_repo, repo.get(state="PARSED"))
        self.assertEqual(get_repo, {"RAPID": {"PARSED": ('2020-06-12_ds1.gz',)},
                                    "CENSYS": {"PARSED": ('2020-06-12_ds2.gz','2020-06-12_ds2_2.gz',)}})

        # Test with STRING paramater
        self.assertRaises(DatasetInvalidError, repo.get, "UNKNOWN", None)
        assert repo.get("RAPID", None)
        self.assertRaises(DatasetInvalidError, repo.get, None, "UNKNOWN")
        assert repo.get( None, "PARSED")

    def test_dumps(self):
        """Test implementation of DatasetRepository method DUMPS and __str__."""
        dp_rapid = DatasetPath(self.TEST_REPO, DatasetType.RAPID, '2020-06-12')
        dp_censys = DatasetPath(self.TEST_REPO, DatasetType.CENSYS, '2020-06-12')
        dp_censys2 = DatasetPath(self.TEST_REPO, DatasetType.CENSYS, '2020-06-30')
        repo = DatasetRepository(self.TEST_REPO)
        # Test DUMPS on empty repository
        assert not repo.dumps()
        assert not str(repo)
        assert not repo.get(dataset_type="RAPID")
        assert not repo.get(state="PARSED")
        assert not repo.get(dataset_id="2020-06-12")
        assert not repo.get("RAPID", "PARSED", "2020-06-12")

        # Fill repository with various datasets
        create_file(os.path.join(self.TEST_REPO, 'ds1.gz'))
        dp_rapid.move(DatasetState.PARSED, os.path.join(self.TEST_REPO, 'ds1.gz'))
        create_file(os.path.join(self.TEST_REPO, 'ds2.gz'))
        dp_censys.move(DatasetState.PARSED, os.path.join(self.TEST_REPO, 'ds2.gz'))
        create_file(os.path.join(self.TEST_REPO, 'ds2_2.gz'))
        dp_censys.move(DatasetState.PARSED, os.path.join(self.TEST_REPO, 'ds2_2.gz'))
        create_file(os.path.join(self.TEST_REPO, 'ds3.gz'))
        dp_censys2.move(DatasetState.VALIDATED, os.path.join(self.TEST_REPO, 'ds3.gz'))

        # Test DUMPS all
        dumps_repo = repo.dumps()
        self.assertEqual(dumps_repo, str(repo))

        # Test DUMPS with specific dataset ID
        self.assertEqual(dumps_repo, repo.dumps(dataset_id=""))
        self.assertEqual(dumps_repo, repo.dumps(dataset_id="2020-06-"))
        assert not repo.dumps(dataset_id='INVALID')
        self.assertNotEqual(dumps_repo, repo.dumps(dataset_id='2020-06-12'))

        # Test DUMPS with specific dataset type
        self.assertNotEqual(dumps_repo, repo.dumps(dataset_type=DatasetType.RAPID))
        self.assertEqual(repo.dumps(dataset_type=DatasetType.RAPID), repo.dumps(dataset_type="RAPID"))

        # Test DUMPS with specific dataset state
        self.assertNotEqual(dumps_repo, repo.dumps(state=DatasetState.PARSED))
        self.assertEqual(repo.dumps(state=DatasetState.PARSED), repo.dumps(state="PARSED"))
        assert not repo.dumps(state=DatasetState.ANALYZED)
        self.assertEqual(repo.dumps(dataset_id='2020-06-12'), repo.dumps(state=DatasetState.PARSED))

        # Test with STRING paramater
        self.assertRaises(DatasetInvalidError, repo.dumps, "UNKNOWN", None)
        assert repo.dumps("RAPID", None)
        self.assertRaises(DatasetInvalidError, repo.dumps, None, "UNKNOWN")
        assert repo.dumps( None, "PARSED")

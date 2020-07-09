"""
This module contains unit tests of Dataset module
"""

import os
import shutil
import unittest
from enum import IntEnum
from cevast.dataset.dataset import Dataset, DatasetState, DatasetType, DatasetInvalidError, DatasetRepository


def create_file(name: str):
    """Create file with given name."""
    os.makedirs(os.path.dirname(name), exist_ok=True)
    with open(name, 'w') as w_file:
        w_file.write("adadadadadasdadadadasda")


class TestDatasetType(unittest.TestCase):
    """Unit test class of DatasetType Enum"""

    def test_validate(self):
        """Test of DatasetType classmethod validate."""
        # Test correct
        assert DatasetType.validate(DatasetType.RAPID)
        assert DatasetType.validate("CENSYS")
        # Test incorrect
        assert not DatasetType.validate(5)
        assert not DatasetType.validate(None)
        assert not DatasetType.validate(DatasetState.UNIFIED)
        # IntEnum is instance of class but not managed
        assert not DatasetType.validate(IntEnum)


class TestDatasetState(unittest.TestCase):
    """Unit test class of DatasetState Enum"""

    def test_validate(self):
        """Test of DatasetState classmethod validate."""
        # Test correct
        assert DatasetState.validate(DatasetState.ANALYSED)
        assert DatasetState.validate("UNIFIED")
        # Test incorrect
        assert not DatasetState.validate(5)
        assert not DatasetState.validate(None)
        assert not DatasetState.validate(DatasetType.RAPID)
        # IntEnum is instance of class but not managed
        assert not DatasetState.validate(IntEnum)


class TestDataset(unittest.TestCase):
    """Unit test class of Dataset class"""

    TEST_REPO = os.path.join('tests', 'test_repository')

    def setUp(self):
        # Create test repository
        os.makedirs(self.TEST_REPO)

    def tearDown(self):
        # Clear test repository
        shutil.rmtree(self.TEST_REPO, ignore_errors=True)

    def test_init(self):
        """Test of Dataset class instantiation."""
        # Test init with wrong parameters
        self.assertRaises(DatasetInvalidError, Dataset, self.TEST_REPO, DatasetState.UNIFIED, '', '443')
        self.assertRaises(DatasetInvalidError, Dataset, self.TEST_REPO, 5, '', '443')
        self.assertRaises(DatasetInvalidError, Dataset, self.TEST_REPO + 'invalid', DatasetType.RAPID, '', '443')
        # Create Dataset instance
        Dataset(self.TEST_REPO, DatasetType.RAPID, '2020-06-12', '443')

        # Test init with STRING type paramater
        Dataset(self.TEST_REPO, "RAPID", '2020-06-12', 443)
        self.assertRaises(DatasetInvalidError, Dataset, self.TEST_REPO, "UNKNOWN", '2020-06-12', '443')

    def test_from_full_path(self):
        """Test of Dataset classmethod from_full_path."""
        # Test incorrect path
        # incorrect repository
        assert Dataset.from_full_path("totally_made_up/RAPID/COLLECTED/66112211_22_suffix.ext") is None
        # incorrect type
        assert Dataset.from_full_path(os.path.join(self.TEST_REPO, "RAPIDOSS/COLLECTED/66112211_22_suffix.ext")) is None
        # incorrect date
        assert Dataset.from_full_path(os.path.join(self.TEST_REPO, "RAPID/COLLECTED/661122_22_suffix.ext")) is None
        # incorrect name
        assert Dataset.from_full_path(os.path.join(self.TEST_REPO, "RAPID/COLLECTED/66112211_.ext")) is None
        assert Dataset.from_full_path(os.path.join(self.TEST_REPO, "RAPID/COLLECTED/66112211")) is None
        assert Dataset.from_full_path(os.path.join(self.TEST_REPO, "RAPID/COLLECTED/66112211_.gz")) is None

        # Test correct path
        dataset = Dataset.from_full_path(os.path.join(self.TEST_REPO, "RAPID/COLLECTED/66112211_22_suffix.ext"))
        assert dataset
        self.assertEqual(dataset.port, "22")
        self.assertEqual(dataset.date, "66112211")
        self.assertEqual(dataset.extension, "ext")
        self.assertEqual(dataset.dataset_type, "RAPID")
        # test without port
        dataset = Dataset.from_full_path(os.path.join(self.TEST_REPO, "RAPID/COLLECTED/66112211_5a_adasd.ext"))
        assert dataset
        self.assertEqual(dataset.port, "")
        path = os.path.abspath(os.path.join(self.TEST_REPO, "RAPID/ANALYSED/66112211_adasd.ext"))
        dataset = Dataset.from_full_path(path)
        assert dataset
        self.assertEqual(dataset.full_path('ANALYSED', 'adasd'), path)
        self.assertEqual(dataset.port, "")
        # test without suffix
        path = os.path.abspath(os.path.join(self.TEST_REPO, "RAPID/COLLECTED/66112211_55.json"))
        dataset = Dataset.from_full_path(path)
        assert dataset
        self.assertEqual(dataset.full_path('COLLECTED'), path)
        self.assertEqual(dataset.port, "55")
        self.assertEqual(dataset.extension, "json")

    def test_path(self):
        """Test implementation of Dataset method PATH."""
        ds_rapid = Dataset(self.TEST_REPO, DatasetType.RAPID, '2020-06-12', '443')
        ds_censys = Dataset(self.TEST_REPO, DatasetType.CENSYS, '2020-06-12', '443')
        # Test GET with wrong state parameter
        self.assertRaises(DatasetInvalidError, ds_rapid.get, 2, False)

        # Test GET with correct state paramater
        path = ds_rapid.path(DatasetState.UNIFIED, False)
        path2 = ds_rapid.path(DatasetState.ANALYSED, False)
        path3 = ds_censys.path(DatasetState.ANALYSED, False)
        assert not os.path.exists(path)
        self.assertNotEqual(path, path2)
        self.assertNotEqual(path2, path3)
        # GET should return /../repository/RAPID/UNIFIED
        self.assertEqual(path, os.path.join(os.path.abspath(self.TEST_REPO), DatasetType.RAPID.name, DatasetState.UNIFIED.name))

        # Test GET with STRING state paramater
        path = ds_rapid.path("UNIFIED", False)
        assert not os.path.exists(path)
        self.assertEqual(path, os.path.join(os.path.abspath(self.TEST_REPO), DatasetType.RAPID.name, DatasetState.UNIFIED.name))

        # Test physically paramater
        assert not os.path.exists(path)
        self.assertEqual(
            ds_rapid.path(DatasetState.UNIFIED, True),
            os.path.join(os.path.abspath(self.TEST_REPO), DatasetType.RAPID.name, DatasetState.UNIFIED.name),
        )
        assert os.path.exists(path)

    def test_full_path(self):
        """Test implementation of Dataset method FULL_PATH."""
        ext = 'ext'
        ds_rapid = Dataset(self.TEST_REPO, DatasetType.RAPID, '2020-06-12', '443', ext)
        ds_censys = Dataset(self.TEST_REPO, DatasetType.CENSYS, '2020-06-30', 443, ext)
        # Test GET_FULL with wrong state parameter
        self.assertRaises(
            DatasetInvalidError, ds_rapid.full_path, 2,
        )

        # Test GET_FULL with correct state paramater
        path = ds_rapid.full_path(DatasetState.UNIFIED, '', False)
        path2 = ds_rapid.full_path(DatasetState.ANALYSED, '', False)
        path3 = ds_censys.full_path(DatasetState.ANALYSED, '', False)
        assert not os.path.exists(path)
        self.assertNotEqual(path, path2)
        self.assertNotEqual(path2, path3)
        # GET_FULL should return /../repository/RAPID/UNIFIED/2020-06-12_443.ext
        self.assertEqual(
            path,
            os.path.join(
                os.path.abspath(self.TEST_REPO), DatasetType.RAPID.name, DatasetState.UNIFIED.name, '2020-06-12_443.' + ext,
            ),
        )
        path = ds_rapid.full_path(DatasetState.UNIFIED, 'suffix', False)
        # GET_FULL should return /../repository/RAPID/UNIFIED/2020-06-12_443_suffix.ext
        self.assertEqual(
            path,
            os.path.join(
                os.path.abspath(self.TEST_REPO),
                DatasetType.RAPID.name,
                DatasetState.UNIFIED.name,
                '2020-06-12_443_suffix.' + ext,
            ),
        )

        # Test GET_FULL without port
        ds_censys = Dataset(self.TEST_REPO, DatasetType.CENSYS, '2020-06-30', '', ext)
        path = ds_censys.full_path(DatasetState.UNIFIED, 'suffix', False)
        # GET_FULL should return /../repository/CENSYS/UNIFIED/2020-06-30_suffix.ext
        self.assertEqual(
            path,
            os.path.join(
                os.path.abspath(self.TEST_REPO), DatasetType.CENSYS.name, DatasetState.UNIFIED.name, '2020-06-30_suffix.' + ext,
            ),
        )
        path = ds_censys.full_path(DatasetState.UNIFIED, '', False)
        # GET_FULL should return /../repository/CENSYS/UNIFIED/2020-06-30.ext
        self.assertEqual(
            path,
            os.path.join(
                os.path.abspath(self.TEST_REPO), DatasetType.CENSYS.name, DatasetState.UNIFIED.name, '2020-06-30.' + ext,
            ),
        )

        # Test GET_FULL with STRING state paramater
        path = ds_rapid.full_path("UNIFIED", '', False)
        assert not os.path.exists(path)
        self.assertEqual(
            path,
            os.path.join(
                os.path.abspath(self.TEST_REPO), DatasetType.RAPID.name, DatasetState.UNIFIED.name, '2020-06-12_443.' + ext,
            ),
        )

        # Test check_if_exists paramater
        assert not os.path.exists(path)
        self.assertEqual(ds_rapid.full_path(DatasetState.UNIFIED, '', True), None)
        os.makedirs(path)
        assert os.path.exists(path)
        self.assertEqual(
            ds_rapid.full_path(DatasetState.UNIFIED, '', True),
            os.path.join(
                os.path.abspath(self.TEST_REPO), DatasetType.RAPID.name, DatasetState.UNIFIED.name, '2020-06-12_443.' + ext,
            ),
        )

    def test_delete(self):
        """Test implementation of Dataset method DELETE."""
        # Test DELETE with not existing file
        ds_rapid = Dataset(self.TEST_REPO, DatasetType.RAPID, '2020-06-12', '443')
        ds_rapid2 = Dataset(self.TEST_REPO, DatasetType.RAPID, '2020-06-30', '443')
        ds_rapid_noport = Dataset(self.TEST_REPO, DatasetType.RAPID, '2020-06-12', '')
        ds_censys = Dataset(self.TEST_REPO, DatasetType.CENSYS, '2020-06-30', None)
        ds_rapid.delete(DatasetState.UNIFIED)

        # Create some datasets
        path = ds_rapid.path(DatasetState.UNIFIED, False)
        create_file(os.path.join(path, '2020-06-12_443.gz'))
        create_file(os.path.join(path, '2020-06-12.gz'))
        create_file(os.path.join(path, '2020-06-30_443.gz'))
        create_file(os.path.join(path, '2020-06-30_443_suffix.gz'))
        assert os.path.exists(os.path.join(path, '2020-06-12_443.gz'))
        assert os.path.exists(os.path.join(path, '2020-06-12.gz'))
        assert os.path.exists(os.path.join(path, '2020-06-30_443.gz'))
        assert os.path.exists(os.path.join(path, '2020-06-30_443_suffix.gz'))
        ds_rapid.delete(DatasetState.ANALYSED)
        # dataset should still exists
        assert os.path.exists(os.path.join(path, '2020-06-12_443.gz'))
        ds_rapid.delete(DatasetState.UNIFIED)
        ds_censys.delete(DatasetState.UNIFIED)
        # dataset should NOT exists but others yes
        assert not os.path.exists(os.path.join(path, '2020-06-12_443.gz'))
        assert os.path.exists(os.path.join(path, '2020-06-12.gz'))
        assert os.path.exists(os.path.join(path, '2020-06-30_443.gz'))
        assert os.path.exists(os.path.join(path, '2020-06-30_443_suffix.gz'))
        ds_rapid2.delete(DatasetState.UNIFIED)
        # only no port dataset should exists
        assert not os.path.exists(os.path.join(path, '2020-06-30_443.gz'))
        assert not os.path.exists(os.path.join(path, '2020-06-30_443_suffix.gz'))
        assert os.path.exists(os.path.join(path, '2020-06-12.gz'))
        # whole dataset state directory should be deleted
        ds_rapid_noport.delete(DatasetState.UNIFIED)
        assert not os.path.exists(path)

        # Test DELETE with STRING state paramater
        path = ds_rapid.path(DatasetState.UNIFIED, False)
        path_cen = ds_censys.path(DatasetState.UNIFIED, False)
        create_file(os.path.join(path, '2020-06-12_443.gz'))
        create_file(os.path.join(path_cen, '2020-06-30.gz'))
        assert os.path.exists(path)
        assert os.path.exists(path_cen)
        self.assertRaises(DatasetInvalidError, ds_rapid.delete, "UNKNOWN")
        self.assertRaises(DatasetInvalidError, ds_censys.delete, "UNKNOWN")
        ds_rapid2.delete("UNIFIED")
        ds_censys.delete("UNIFIED")
        assert os.path.exists(path)
        assert not os.path.exists(path_cen)
        ds_rapid.delete("UNIFIED")
        assert not os.path.exists(path)

    def test_purge(self):
        """Test implementation of Dataset method PURGE."""
        ds_rapid = Dataset(self.TEST_REPO, DatasetType.RAPID, '2020-06-12', '')
        ds_censys = Dataset(self.TEST_REPO, DatasetType.CENSYS, '2020-06-12', '')
        path_r = os.path.join(self.TEST_REPO, DatasetType.RAPID.name)
        path_c = os.path.join(self.TEST_REPO, DatasetType.CENSYS.name)
        # Test PURGE on empty repository
        ds_rapid.purge()
        # Create some datasets and PURGE rapid repository
        create_file(os.path.join(path_r, '2020-06-12.gz'))
        create_file(os.path.join(path_c, '2020-06-12.gz'))
        assert os.path.exists(path_r)
        assert os.path.exists(path_c)
        ds_rapid.purge()
        assert not os.path.exists(path_r)
        assert os.path.exists(path_c)
        # now purge also the other repository
        ds_censys.purge()
        assert not os.path.exists(path_r)
        assert not os.path.exists(path_c)
        assert not os.listdir(self.TEST_REPO)

    def test_get(self):
        """Test implementation of Dataset method GET."""
        ds_rapid = Dataset(self.TEST_REPO, DatasetType.RAPID, '2020-06-12', None)
        ds_censys = Dataset(self.TEST_REPO, DatasetType.CENSYS, '2020-06-12', '22')
        path_r = os.path.join(self.TEST_REPO, DatasetType.RAPID.name, DatasetState.UNIFIED.name)
        path_c = os.path.join(self.TEST_REPO, DatasetType.CENSYS.name, DatasetState.UNIFIED.name)
        assert not ds_rapid.get(DatasetState.UNIFIED)
        assert not ds_censys.get(DatasetState.UNIFIED)
        create_file(os.path.join(path_r, '2020-06-12.gz'))
        create_file(os.path.join(path_r, '2020-06-12_suffix.gz'))
        create_file(os.path.join(path_c, '2020-06-12.gz'))
        create_file(os.path.join(path_c, '2020-06-12_22.gz'))

        # Check filter by state only
        assert not ds_rapid.get(DatasetState.ANALYSED)
        assert not ds_censys.get(DatasetState.ANALYSED)

        # Check filter by port
        get_rapid = ds_rapid.get(DatasetState.UNIFIED)
        self.assertEqual(get_rapid, ('2020-06-12.gz', '2020-06-12_suffix.gz'))
        get_censys = ds_censys.get(DatasetState.UNIFIED)
        self.assertEqual(get_censys, ('2020-06-12_22.gz',))

        # Check filter by suffix
        get_rapid = ds_rapid.get(DatasetState.UNIFIED, 'suffix')
        self.assertEqual(get_rapid, ('2020-06-12_suffix.gz',))
        assert not ds_censys.get(DatasetState.UNIFIED, '22')

    def test_exists(self):
        """Test implementation of Dataset method EXISTS and EXISTS_ANY."""
        ds_rapid = Dataset(self.TEST_REPO, DatasetType.RAPID, '2020-06-12', None)
        ds_censys = Dataset(self.TEST_REPO, DatasetType.CENSYS, '2020-06-12', '22')
        path_r = os.path.join(self.TEST_REPO, DatasetType.RAPID.name, DatasetState.UNIFIED.name)
        path_c = os.path.join(self.TEST_REPO, DatasetType.CENSYS.name, DatasetState.UNIFIED.name)
        assert not ds_rapid.exists_any()
        assert not ds_rapid.exists_any()
        assert not ds_censys.exists(DatasetState.UNIFIED)
        # Create RAPID dataset and check if exists
        create_file(os.path.join(path_r, '2020-06-12.gz'))
        assert ds_rapid.exists_any()
        assert ds_rapid.exists(DatasetState.UNIFIED)
        assert not ds_rapid.exists(DatasetState.ANALYSED)
        assert not ds_censys.exists_any()
        # create different dataset ID that should NOT exists
        create_file(os.path.join(path_c, '2020-06-30_suffix.gz'))
        create_file(os.path.join(path_c, '2020-06-12_11_suffix.gz'))
        assert not ds_censys.exists_any()
        assert not ds_censys.exists(DatasetState.UNIFIED)
        # now create correct CENSYS dataset and check if exists
        create_file(os.path.join(path_c, '2020-06-12_22_suffix.gz'))
        assert ds_censys.exists_any()
        assert ds_censys.exists(DatasetState.UNIFIED)
        assert not ds_censys.exists(DatasetState.ANALYSED)

        # Test STRING state paramater
        assert ds_censys.exists("UNIFIED")
        self.assertRaises(DatasetInvalidError, ds_censys.exists, "UNKNOWN")

    def test_move(self):
        """Test implementation of Dataset method MOVE."""
        ds_rapid = Dataset(self.TEST_REPO, DatasetType.RAPID, '2020-06-12', '443')
        path = os.path.join(self.TEST_REPO, DatasetType.RAPID.name, DatasetState.UNIFIED.name)
        dataset = os.path.join(self.TEST_REPO, '2020-06-30_suffix.gz')
        ds_suffix_only = os.path.join(self.TEST_REPO, 'suffix.gz')
        # Test with source that doesn't exist
        ds_rapid.move(DatasetState.UNIFIED, "totally_made_up")
        assert not os.path.exists(path)

        # Create dataset and move it
        create_file(ds_suffix_only)
        assert os.path.exists(ds_suffix_only)
        ds_rapid.move(DatasetState.UNIFIED, ds_suffix_only)
        assert os.path.exists(path)
        assert not os.path.exists(ds_suffix_only)
        assert os.path.exists(os.path.join(path, '2020-06-12_443_suffix.gz'))

        # Create dataset and move it without prefix
        create_file(dataset)
        assert os.path.exists(dataset)
        ds_rapid.move(DatasetState.UNIFIED, dataset, False)
        assert not os.path.exists(dataset)
        assert os.path.exists(os.path.join(path, '2020-06-30_suffix.gz'))

        # Test with STRING state paramater
        ds_rapid = Dataset(self.TEST_REPO, DatasetType.CENSYS, '2020-06-30', '')
        create_file(ds_suffix_only)
        self.assertRaises(DatasetInvalidError, ds_rapid.move, "UNKNOWN", ds_suffix_only)
        self.assertRaises(DatasetInvalidError, ds_rapid.move, "UNKNOWN", ds_suffix_only, False)
        ds_rapid.move("ANALYSED", ds_suffix_only)
        assert not os.path.exists(ds_suffix_only)
        assert os.path.exists(
            os.path.join(self.TEST_REPO, DatasetType.CENSYS.name, DatasetState.ANALYSED.name, '2020-06-30_suffix.gz')
        )

    def test_str(self):
        """Test implementation of Dataset override method __str__."""
        ds_rapid = Dataset(self.TEST_REPO, DatasetType.RAPID, '2020-06-12', '')
        # string representation should look like: repository/type/{placeholder for state}/id
        self.assertEqual(str(ds_rapid).format('COLLECTED'), os.path.join(ds_rapid.path('COLLECTED', False), '2020-06-12'))

    def test_hashable(self):
        """Test implementation of Dataset override method __hash__ and __eq__."""
        rapid = Dataset(self.TEST_REPO, DatasetType.RAPID, '2020-06-30', '443')
        rapid_eq = Dataset(self.TEST_REPO, DatasetType.RAPID, '2020-06-30', '443')
        rapid_1 = Dataset(self.TEST_REPO, DatasetType.RAPID, '2020-06-16', '')
        rapid_2 = Dataset(self.TEST_REPO, DatasetType.RAPID, '2020-06-30', '22')
        censys = Dataset(self.TEST_REPO, DatasetType.CENSYS, '2020-06-30', '')
        # Test equal
        self.assertEqual(rapid, rapid_eq)
        self.assertNotEqual(rapid, rapid_1)
        self.assertNotEqual(rapid, rapid_2)
        self.assertNotEqual(rapid_1, rapid_2)
        self.assertNotEqual(rapid, censys)
        # Test hash
        self.assertEqual(hash(rapid), hash(rapid_eq))
        self.assertNotEqual(hash(rapid), hash(rapid_1))
        self.assertNotEqual(hash(rapid), hash(rapid_2))
        self.assertNotEqual(hash(rapid_1), hash(rapid_2))
        self.assertNotEqual(hash(rapid), hash(censys))
        # Test use of hashable
        assert rapid in tuple((rapid,))
        assert rapid in tuple((rapid_1, rapid_eq))
        assert rapid not in tuple((rapid_1, rapid_2, censys))
        assert censys in tuple((rapid_1, rapid_2, censys))


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
        self.assertRaises(FileNotFoundError, DatasetRepository, None)
        # Create DatasetRepository instance
        DatasetRepository(self.TEST_REPO)
        DatasetRepository('.')

    def test_get(self):
        """Test implementation of DatasetRepository method GET."""
        ds_rapid = Dataset(self.TEST_REPO, DatasetType.RAPID, '2020-06-12', '')
        ds_censys = Dataset(self.TEST_REPO, DatasetType.CENSYS, '2020-06-12', '443')
        ds_censys2 = Dataset(self.TEST_REPO, DatasetType.CENSYS, '2020-06-30', '')
        repo = DatasetRepository(self.TEST_REPO)
        # Test GET on empty repository
        assert not repo.get()
        assert not repo.get(dataset_type="RAPID")
        assert not repo.get(state="UNIFIED")
        assert not repo.get(dataset_id="2020-06-12")
        assert not repo.get("RAPID", "UNIFIED", "2020-06-12")

        # Fill repository with various datasets
        create_file(os.path.join(self.TEST_REPO, 'ds1.gz'))
        ds_rapid.move(DatasetState.UNIFIED, os.path.join(self.TEST_REPO, 'ds1.gz'))
        create_file(os.path.join(self.TEST_REPO, 'ds2.gz'))
        ds_censys.move(DatasetState.UNIFIED, os.path.join(self.TEST_REPO, 'ds2.gz'))
        create_file(os.path.join(self.TEST_REPO, 'ds2_2.gz'))
        ds_censys.move(DatasetState.UNIFIED, os.path.join(self.TEST_REPO, 'ds2_2.gz'))
        create_file(os.path.join(self.TEST_REPO, 'ds3.gz'))
        ds_censys2.move(DatasetState.ANALYSED, os.path.join(self.TEST_REPO, 'ds3.gz'))

        # Test GET all
        get_repo = repo.get()
        self.assertEqual(
            get_repo,
            {
                "RAPID": {"UNIFIED": ('2020-06-12_ds1.gz',)},
                "CENSYS": {
                    "UNIFIED": ('2020-06-12_443_ds2.gz', '2020-06-12_443_ds2_2.gz',),
                    "ANALYSED": ('2020-06-30_ds3.gz',),
                },
            },
        )
        # Test GET with specific dataset ID
        self.assertEqual(get_repo, repo.get(dataset_id=""))
        self.assertEqual(get_repo, repo.get(dataset_id="2020-06-"))
        assert not repo.get(dataset_id='INVALID')
        get_repo = repo.get(dataset_id='2020-06-12')
        self.assertEqual(
            get_repo,
            {
                "RAPID": {"UNIFIED": ('2020-06-12_ds1.gz',)},
                "CENSYS": {"UNIFIED": ('2020-06-12_443_ds2.gz', '2020-06-12_443_ds2_2.gz',)},
            },
        )

        # Test GET with specific dataset type
        get_repo = repo.get(dataset_type=DatasetType.RAPID)
        self.assertEqual(get_repo, repo.get(dataset_type="RAPID"))
        self.assertEqual(get_repo, {"RAPID": {"UNIFIED": ('2020-06-12_ds1.gz',)}})

        # Test GET with specific dataset state
        assert not repo.get(state=DatasetState.FILTERED)
        get_repo = repo.get(state=DatasetState.UNIFIED)
        self.assertEqual(get_repo, repo.get(state="UNIFIED"))
        self.assertEqual(
            get_repo,
            {
                "RAPID": {"UNIFIED": ('2020-06-12_ds1.gz',)},
                "CENSYS": {"UNIFIED": ('2020-06-12_443_ds2.gz', '2020-06-12_443_ds2_2.gz',)},
            },
        )

        # Test with STRING paramater
        self.assertRaises(DatasetInvalidError, repo.get, "UNKNOWN", None)
        assert repo.get("RAPID", None)
        self.assertRaises(DatasetInvalidError, repo.get, None, "UNKNOWN")
        assert repo.get(None, "UNIFIED")

    def test_dumps(self):
        """Test implementation of DatasetRepository method DUMPS and __str__."""
        ds_rapid = Dataset(self.TEST_REPO, DatasetType.RAPID, '2020-06-12', '')
        ds_censys = Dataset(self.TEST_REPO, DatasetType.CENSYS, '2020-06-12', '')
        ds_censys2 = Dataset(self.TEST_REPO, DatasetType.CENSYS, '2020-06-30', '')
        repo = DatasetRepository(self.TEST_REPO)
        # Test DUMPS on empty repository
        assert not repo.dumps()
        assert not str(repo)
        assert not repo.get(dataset_type="RAPID")
        assert not repo.get(state="UNIFIED")
        assert not repo.get(dataset_id="2020-06-12")
        assert not repo.get("RAPID", "UNIFIED", "2020-06-12")

        # Fill repository with various datasets
        create_file(os.path.join(self.TEST_REPO, 'ds1.gz'))
        ds_rapid.move(DatasetState.UNIFIED, os.path.join(self.TEST_REPO, 'ds1.gz'))
        create_file(os.path.join(self.TEST_REPO, 'ds2.gz'))
        ds_censys.move(DatasetState.UNIFIED, os.path.join(self.TEST_REPO, 'ds2.gz'))
        create_file(os.path.join(self.TEST_REPO, 'ds2_2.gz'))
        ds_censys.move(DatasetState.UNIFIED, os.path.join(self.TEST_REPO, 'ds2_2.gz'))
        create_file(os.path.join(self.TEST_REPO, 'ds3.gz'))
        ds_censys2.move(DatasetState.ANALYSED, os.path.join(self.TEST_REPO, 'ds3.gz'))

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
        self.assertNotEqual(dumps_repo, repo.dumps(state=DatasetState.UNIFIED))
        self.assertEqual(repo.dumps(state=DatasetState.UNIFIED), repo.dumps(state="UNIFIED"))
        assert not repo.dumps(state=DatasetState.FILTERED)
        self.assertEqual(repo.dumps(dataset_id='2020-06-12'), repo.dumps(state=DatasetState.UNIFIED))

        # Test with STRING paramater
        self.assertRaises(DatasetInvalidError, repo.dumps, "UNKNOWN", None)
        assert repo.dumps("RAPID", None)
        self.assertRaises(DatasetInvalidError, repo.dumps, None, "UNKNOWN")
        assert repo.dumps(None, "UNIFIED")

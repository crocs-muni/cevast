"""
This module contains unit tests of cevast.dataset.collectors package.
"""

import os
import unittest
import unittest.mock
import datetime
from cevast.dataset.collectors import RapidCollector


class TestRapidCollector(unittest.TestCase):
    """Unit test class of RapidCollector class"""

    def test_collect(self):
        """
        Test implementation of RapidCollector method COLLECT
        """
        collector = RapidCollector('api_key')
        # Mock method get_datasets() and __download()
        collector.get_datasets = unittest.mock.MagicMock()
        collector._RapidCollector__download = unittest.mock.MagicMock()

        # Test collect on empty dataset list
        collector.get_datasets.return_value = []
        self.assertEqual(collector.collect(), ())
        collector.get_datasets.assert_called_once()
        assert not collector._RapidCollector__download.called

        collector.get_datasets.return_value = [
            '20200613/2018-02-13-1518483602-https_get_443_certs.gz',
            '20200612_ssl_443_names.gz',
            '20200609_ssl_22_names.gz',
            '20200609_ssl_443_certs.gz',
        ]
        # Test date filter
        res = collector.collect(date=datetime.date(2020, 6, 17), filter_ports=None, filter_types=None)
        # the newest dataset should match
        self.assertEqual(res, (os.path.normcase('./20200613_443_certs.gz'),))
        res_nodate = collector.collect(filter_ports=None, filter_types=None)
        self.assertEqual(res_nodate, (os.path.normcase('./20200613_443_certs.gz'),))
        self.assertEqual(res, res_nodate)
        # exact date should be collected
        res_12 = collector.collect(date=datetime.date(2020, 6, 12), filter_ports=None, filter_types=None)
        self.assertEqual(res_12, (os.path.normcase('./20200612_443_names.gz'),))
        res_11 = collector.collect(date=datetime.date(2020, 6, 11), filter_ports=None, filter_types=None)
        res_9 = collector.collect(date=datetime.date(2020, 6, 9), filter_ports=None, filter_types=None)
        self.assertEqual(res_11, tuple(map(os.path.normcase, ('./20200609_22_names.gz', './20200609_443_certs.gz'))))
        self.assertEqual(res_11, res_9)
        # dataset newer that this date should not be collected
        res_8 = collector.collect(date=datetime.date(2020, 6, 8), filter_ports=None, filter_types=None)
        self.assertEqual(res_8, ())

        # Test filter_ports paramater
        res = collector.collect(filter_ports='22', filter_types=None)
        self.assertEqual(res, (os.path.normcase('./20200609_22_names.gz'),))
        res = collector.collect(date=datetime.date(2020, 6, 9), filter_ports=['22', '443'], filter_types=None)
        self.assertEqual(res, tuple(map(os.path.normcase, ('./20200609_22_names.gz', './20200609_443_certs.gz'))))
        # everything should be filtered out
        res = collector.collect(filter_ports='XXX', filter_types=None)
        self.assertEqual(res, ())

        # Test filter_types paramater
        res = collector.collect(date=datetime.date(2020, 6, 9), filter_ports=None, filter_types='names')
        self.assertEqual(res, (os.path.normcase('./20200609_22_names.gz'),))
        res = collector.collect(date=datetime.date(2020, 6, 9), filter_ports=None, filter_types=['names', 'certs'])
        self.assertEqual(res, tuple(map(os.path.normcase, ('./20200609_22_names.gz', './20200609_443_certs.gz'))))
        # everything should be filtered out
        res = collector.collect(filter_ports=None, filter_types=['XXX', 'X'])
        self.assertEqual(res, ())

        # Test not supported format of dataset names
        collector.get_datasets.return_value = [
            '2020-06-13/2018-02-13-1518483602-https_get_443_certs.gz',
            '20200612_ss_names.gz',
            '20200609_ssl_22.gz',
            '202006_ssl_22_certs.gz',
            '20200609_ssl_443_certs.json',
        ]
        res = collector.collect(filter_ports=None, filter_types=None)
        self.assertEqual(res, ())

"""This module contains collector implementation for Rapid dataset type."""
# pylint: disable=E1101
#   - known Pylint issue https://github.com/PyCQA/pylint/issues/1411

import os
import logging
import re
from datetime import datetime
from typing import List, Union, Tuple
import requests
from cevast.dataset.dataset import DatasetType, DatasetCollectionError, Dataset

__author__ = 'Radim Podola'

log = logging.getLogger(__name__)


class RapidCollector:
    """
    Class collecting datasets from Rapid7 project Sonar study: "Project Sonar: IPv4 SSL Certificates".

    Data are retrieved via Open Data API (more at: https://opendata.rapid7.com/apihelp/).
    To use the API to collect the datasets, one must have an account and API User Key.
    Set the key to "RAPID_API_KEY" enviroment variable or provide upon initialization.
    """

    dataset_type = DatasetType.RAPID.name
    _DATASETS_URL = "https://us.api.insight.rapid7.com/opendata/studies/sonar.ssl/"
    _DOWNLOAD_URL = _DATASETS_URL + "{}/download/"
    _QUOTA_URL = "https://us.api.insight.rapid7.com/opendata/quota/"

    def __init__(self, api_key: str = None):
        if api_key is None:
            try:
                self.__api_key = os.environ['RAPID_API_KEY']
            except KeyError:
                log.error('RAPID_API_KEY is not set in enviroment!')
                raise ValueError('RAPID_API_KEY is not provided nor set in the enviroment!')
        else:
            self.__api_key = api_key

    # TODO add date_range strategy via paramater date_since=None
    def collect(self, download_dir: str = '.', date: datetime.date = datetime.today().date(),
                filter_ports: Union[Tuple[str], str] = '443',
                filter_types: Union[Tuple[str], str] = ('hosts', 'certs')) -> Tuple[str]:
        """
        Download the newest dataset by the given date and store it into `download_dir` directory.
        If `date` is not specified, the newest dataset ever is downloaded by default.
        Datasets can be filtered by PORT and TYPE via paramaters `filter_ports` and `filter_types`.
        If a filter parameter is empty or None, the filter is not applied.

        Tuple with downloaded dataset's paths is returned.

        Function first query Open Data API and get fresh list of all datasets. If filter parameters
        are given, list if filtered accordingly. Datasets are provided chronologically in a list
        from the newest to the oldest ones. List is iterated from the newest until the closest
        dataset to given date is reached.

        Most of the dataset names has a fixed format: "{YYYYmmdd}/...._{PORT}_{TYPE}.gz", where
            {YYYYmmdd} is the date when the dataset was collected,
            {PORT} is port on which the certificates were retrieved,
            {TYPE} is type of the dataset [hosts, endpoints, names, certs]
            e.g. 20180213/2018-02-13-1518483602-https_get_443_certs.gz or
                 20170410_ssl_443_names.gz
        Older datasets with different formats are not supported now.
        """

        def match_filters(dataset: str) -> bool:
            match = re.match(dataset_name_rgx, dataset)
            if not match:
                return False
            if filter_ports and match.group('port') not in filter_ports:
                return False
            if filter_types and match.group('type') not in filter_types:
                return False
            return True

        dataset_name_rgx = r"^(?P<date>\d{8}).*_(?P<port>\d+)_(?P<type>\w+)\.gz$"
        datasets_to_download = {}
        target_date = None
        log.info('Start collecting Rapid datasets with paramaters:')
        log.info('  date=%s, ports=%s, types=%s to directory %s', date, filter_ports, filter_types, download_dir)
        # Filter the datasets
        datasets = list(filter(match_filters, self.get_datasets()))
        log.debug('Filtered datasets: %s', datasets)
        for dataset in datasets:
            if target_date is None and date >= datetime.strptime(dataset[:8], '%Y%m%d').date():
                # Found the target date
                target_date = dataset[:8]
            if target_date is not None:
                match = re.match(dataset_name_rgx, dataset)
                if match.group('date') != target_date:
                    break  # Another date encountered, we have all datasets now -> break
                path = os.path.join(
                    download_dir,
                    Dataset.format_filename(match.group('date'), match.group('port'), match.group('type') + '.gz'),
                )
                datasets_to_download[dataset] = path
        # Download the datasets
        for dataset_file, path in datasets_to_download.items():
            log.info('Download dataset file <%s> to <%s>.', dataset_file, path)
            if os.path.exists(path):
                log.info('Dataset is already downloaded.')
            else:
                self.__download(dataset_file, path)

        log.info('Rapid datasets collected.')
        return tuple(datasets_to_download.values())

    def get_datasets(self) -> List[str]:
        """
        Get fresh list of datasets in the project study.
        """
        req = requests.get(self._DATASETS_URL, headers={"X-Api-Key": self.__api_key})
        if req.status_code != requests.codes.ok:
            log.error('HTTP error <%s> when retrieving dataset list.', req.status_code)
            return []
        return req.json().get('sonarfile_set', [])

    def get_quota(self) -> dict:
        """Get account download quota information."""
        req = requests.get(self._QUOTA_URL, headers={"X-Api-Key": self.__api_key})
        if req.status_code != requests.codes.ok:
            log.error('HTTP error <%s> when retrieving download quota information.', req.status_code)
            return {}
        return req.json()

    def quota_left(self) -> int:
        """Return how many download quota is left for the day."""
        return self.get_quota().get('quota_left', 0)

    def __download(self, dataset_name: str, target_filename: str):
        """Download the dataset from the project study."""
        # Retrieve download URL of the dataset archive
        req = requests.get(self._DOWNLOAD_URL.format(dataset_name), headers={"X-Api-Key": self.__api_key})
        if req.status_code != requests.codes.ok:
            log.error('HTTP error <%s> when retrieving download URL.', req.status_code)
            retry = req.headers.get('Retry-After', 0)
            raise DatasetCollectionError('Dataset download failed. Quota might exceeded, try again after {}[s].'.format(retry))
        url = req.json().get('url', None)
        if not url:
            raise DatasetCollectionError('Dataset download failed.')
        # Download the dataset archive
        with requests.get(url, stream=True) as stream_f:
            if stream_f.status_code != requests.codes.ok:
                raise DatasetCollectionError('Dataset download failed with HTTP error <{}>.'.format(stream_f.status_code))
            # Dataset file might have size of GBs, so read and write by increments
            log.debug('Downloading dataset of size %sB.', stream_f.headers['Content-Length'])
            with open(target_filename, 'wb') as w_obj:
                for chunk in stream_f.iter_content(chunk_size=8192):
                    w_obj.write(chunk)
                    # TODO make some progress logs (25%, 50%, 75%)

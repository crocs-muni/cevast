"""This module contains DatasetManager interface implementation of RAPID dataset type."""

import os
import logging
import json
from collections import OrderedDict
from datetime import datetime
from typing import Tuple, Optional
from cevast.certdb import CertDB
from .manager import DatasetManager, DatasetManagerTask
from ..parsers import RapidParser
from ..collectors import RapidCollector
from ..dataset import DatasetType, Dataset, DatasetState, DatasetRepository, DatasetParsingError

__author__ = 'Radim Podola'

log = logging.getLogger(__name__)


class RapidDatasetManager(DatasetManager):
    """DatasetManager interface implementation of RAPID dataset type."""

    _CERT_NAME_SUFFIX = 'certs'
    _HOSTS_NAME_SUFFIX = 'hosts'
    _CHAINS_NAME_SUFFIX = 'chains'
    _BROKEN_CHAINS_NAME_SUFFIX = 'broken_chains'

    dataset_type = DatasetType.RAPID.name

    # TODO add date range
    def __init__(self, repository: str, date: datetime.date = datetime.today().date(),
                 ports: Tuple[str] = ('443',), cpu_cores: int = 1):
        self._repository = repository
        self._date = date
        self._ports = ports
        self._cpu_cores = cpu_cores
        self.__date_id = date.strftime('%Y%m%d')
        self.__dataset_path_any_port = Dataset(self._repository, self.dataset_type, self.__date_id, None)
        self.__dataset_repo = DatasetRepository(repository)
        log.info('RapidDatasetManager initialized with repository=%s, date=%s, cpu_cores=%s', repository, date, cpu_cores)

    def run(self, task_pipline: Tuple[DatasetManagerTask], certdb: Optional[CertDB]) -> bool:
        collected_datasets = self.collect()
        log.info("%s", str(collected_datasets))
        parsed_datasets = self.__parse(certdb=certdb, datasets=collected_datasets)
        log.info("%s", parsed_datasets)
        return True

    def collect(self, api_key: str = None) -> Tuple[Dataset]:
        log.info('Collecting started')
        collector = RapidCollector(api_key)
        download_dir = self.__dataset_path_any_port.path(DatasetState.COLLECTED)
        # Collect datasets
        collected = collector.collect(download_dir=download_dir, date=self._date,
                                      filter_ports=self._ports, filter_types=('hosts', 'certs'))
        # Remove duplicates (same datasets with e.g. different suffix)
        datasets = list(OrderedDict.fromkeys(map(Dataset.from_full_path, collected)))
        log.info('%d dataset were downloaded', len(datasets))
        log.info('Collecting finished')
        return datasets

    def analyse(self, methods: list = None) -> str:
        raise NotImplementedError

    def parse(self, certdb: CertDB) -> Tuple[str]:
        log.info('Parsing started')
        # if not self._ports:
        #    self.__dataset_path_any_port.get(DatasetState.COLLECTED)
        # ...
        # else:
        datasets = tuple(Dataset(self._repository, self.dataset_type, self.__date_id, port) for port in self._ports)
        # Parse datasets
        parsed = self.__parse(certdb=certdb, datasets=datasets)
        log.info('Parsing finished')
        return parsed

    def __init_parser(self, dataset: Dataset) -> RapidParser:
        certs_file = dataset.full_path(DatasetState.COLLECTED, self._CERT_NAME_SUFFIX, True)
        hosts_file = dataset.full_path(DatasetState.COLLECTED, self._HOSTS_NAME_SUFFIX, True)
        if certs_file and hosts_file:
            chain_file = dataset.full_path(DatasetState.PARSED, self._CHAINS_NAME_SUFFIX, physically=True)
            broken_file = dataset.full_path(DatasetState.PARSED, self._BROKEN_CHAINS_NAME_SUFFIX, physically=True)
            try:
                parser = RapidParser(certs_file, hosts_file, chain_file, broken_file)
                log.info("Will parse dataset: %s", dataset.static_filename)
                return parser
            except FileNotFoundError:
                log.exception("Collected dataset not found")
        return None

    def __parse(self, certdb: CertDB, datasets: Tuple[Dataset], commit: bool = True, store_log: bool = True) -> Tuple[str]:
        # First init parsers
        parsers = tuple(self.__init_parser(d) for d in datasets if d is not None)
        # Parse and store certificates
        for parser in parsers:
            try:
                parser.store_certs(certdb)
            except (OSError, ValueError):
                log.exception("Error during certs dataset parsing -> rollback and exit")
                certdb.rollback()
                return None
        # Now parse and store chains
        for parser in parsers:
            try:
                parser.store_chains(certdb)
                if store_log:
                    # Store dataset parsing log
                    log_name = os.path.splitext(parser.chain_file)[0] + '.log'
                    log_str = json.dumps(parser.parsing_log, sort_keys=True, indent=4)
                    log.info('Storing parsing log: %s', log_name)
                    log.debug('%s', log_str)
                    with open(log_name, 'w') as outfile:
                        outfile.write(log_str)
            except OSError:
                log.exception("Error during hosts dataset parsing -> commit and return")
                certdb.commit()
                raise DatasetParsingError("Error during hosts dataset parsing")
        # Commit inserted certificates now (ertdb.exists_all is faster before commit)
        if commit:
            certdb.commit()
        # Remove parsed datasets
        for dataset in datasets:
            dataset.delete(DatasetState.COLLECTED)
        return tuple(parser.chain_file for parser in parsers)

    def validate(self, database: CertDB, validation_cfg: dict) -> str:
        pass

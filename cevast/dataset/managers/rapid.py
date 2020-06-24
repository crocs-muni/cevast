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
    def __init__(self, repository: str, date: datetime.date = datetime.today().date(), ports: Tuple[str] = ('443',)):
        self._repository = repository
        self._date = date
        self._ports = ports
        self.__date_id = date.strftime('%Y%m%d')
        self.__dataset_path_any_port = Dataset(self._repository, self.dataset_type, self.__date_id, None)
        self.__dataset_repo = DatasetRepository(repository)
        log.info('RapidDatasetManager initialized with repository=%s, date=%s', repository, date)

    def run(self, task_pipline: Tuple[Tuple[DatasetManagerTask, dict]]) -> None:
        collected_datasets, parsed_datasets = None, None
        # Sort just to ensure valid sequence
        task_pipline = sorted(task_pipline, key=lambda x: x[0])
        log.info('Started with task pipeline %s', task_pipline)
        # Run tasks
        for task_item in task_pipline:
            task, params = task_item
            log.info('Run task %s with parameters: %s', task, params)
            # Runs collection TASK, collected datasets might be used in next task
            if task == DatasetManagerTask.COLLECT:
                collected_datasets = self.collect(**params)
                log.info("Collected datasets: %s", collected_datasets)

            # Runs analyzing TASK
            elif task == DatasetManagerTask.ANALYSE:
                pass  # Not implemented yet

            # Runs parsing TASK, parsed datasets might be used in next task
            elif task == DatasetManagerTask.PARSE:
                if collected_datasets is None:
                    parsed_datasets = self.parse(**params)
                else:  # If some datasets were just collected, use these
                    parsed_datasets = self.__parse(datasets=collected_datasets, **params)
                log.info("Parsed datasets: %s", parsed_datasets)

            # Runs validation TASK
            elif task == DatasetManagerTask.VALIDATE:
                if parsed_datasets is None:
                    validated_datasets = self.validate(**params)
                else:
                    validated_datasets = self.__validate(datasets=parsed_datasets, **params)
                log.info("Validated datasets: %s", validated_datasets)
        log.info("Finished")

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

    def parse(self, certdb: CertDB) -> Tuple[Dataset]:
        log.info('Parsing started')
        # if not self._ports:
        #    self.__dataset_path_any_port.get(DatasetState.COLLECTED)
        # ...
        # else:
        datasets = self.__init_datasets()
        # Parse datasets
        parsed = self.__parse(certdb=certdb, datasets=datasets)
        log.info('Parsing finished')
        return parsed

    def validate(self, certdb: CertDB, validator: object, validator_cfg: dict, cpu_cores: int = 1) -> Tuple[Dataset]:
        log.info('Validation started')
        datasets = self.__init_datasets()
        # Validate datasets
        validated = self.__validate(certdb=certdb,
                                    datasets=datasets,
                                    validator=validator,
                                    validator_cfg=validator_cfg,
                                    cpu_cores=cpu_cores)
        log.info('Validation finished')
        return validated

    def __init_datasets(self) -> Tuple[Dataset]:
        return tuple(Dataset(self._repository, self.dataset_type, self.__date_id, port) for port in self._ports)

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

    def __parse(self, certdb: CertDB, datasets: Tuple[Dataset], store_log: bool = True) -> Tuple[Dataset]:
        # First validate datasets and init parsers
        parsable, parsers = [], []
        for dataset in datasets:
            parser = self.__init_parser(dataset)
            if parser is not None:
                parsers.append(parser)
                parsable.append(dataset)
        # Parse and store certificates
        for parser in parsers:
            try:
                parser.store_certs(certdb)
            except (OSError, ValueError):
                log.exception("Error during certs dataset parsing -> rollback")
                certdb.rollback()
                raise DatasetParsingError("Error during certs dataset parsing")
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
                log.exception("Error during hosts dataset parsing -> commit")
                certdb.commit()
                raise DatasetParsingError("Error during hosts dataset parsing")
        # Remove parsed datasets
        for dataset in parsable:
            dataset.delete(DatasetState.COLLECTED)
        return tuple(parsable)

    def __validate(self, datasets: Tuple[Dataset], certdb: CertDB,
                   validator: object, validator_cfg: dict, cpu_cores: int = 1) -> Tuple[Dataset]:
        res = validator(["cert_mock"], validator_cfg)
        print(res)
        return datasets

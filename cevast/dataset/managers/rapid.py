"""This module contains DatasetManager interface implementation of RAPID dataset type."""

import os
import logging
from collections import OrderedDict
from datetime import datetime
from typing import Tuple
from cevast.certdb import CertDB
from .manager import DatasetManager, DatasetManagerTask
from ..parsers import RapidParser
from ..collectors import RapidCollector
from ..dataset import DatasetType, Dataset, DatasetState, DatasetParsingError

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
    # TODO make ports optional
    def __init__(self, repository: str, date: datetime.date = datetime.today().date(),
                 ports: Tuple[str] = ('443',), cpu_cores: int = 1):
        self._repository = repository
        self._date = date
        self._ports = (ports,) if isinstance(ports, str) else ports
        self._cpu_cores = cpu_cores
        self.__date_id = date.strftime('%Y%m%d')
        self.__dataset_path_any_port = Dataset(self._repository, self.dataset_type, self.__date_id, None)
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
                if collected_datasets:
                    parsed_datasets = self.parse(**params)
                else:  # If some datasets were just collected, use these
                    parsed_datasets = self.__parse(datasets=collected_datasets, **params)
                log.info("Parsed datasets: %s", parsed_datasets)

            # Runs validation TASK
            elif task == DatasetManagerTask.VALIDATE:
                if parsed_datasets:
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
        collected = collector.collect(
            download_dir=download_dir, date=self._date, filter_ports=self._ports, filter_types=('hosts', 'certs')
        )
        # Remove duplicates (same datasets with e.g. different suffix)
        datasets = tuple(OrderedDict.fromkeys(map(Dataset.from_full_path, collected)))
        log.info('%d dataset were downloaded', len(datasets))
        log.info('Collecting finished')
        return datasets if datasets else None

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
        return parsed if parsed else None

    def validate(self, certdb: CertDB, validator: object, validator_cfg: dict) -> Tuple[Dataset]:
        log.info('Validation started')
        datasets = self.__init_datasets()
        # Validate datasets
        validated = self.__validate(
            certdb=certdb, datasets=datasets, validator=validator, validator_cfg=validator_cfg
        )
        log.info('Validation finished')
        return validated if validated else None

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
                    parser.save_parsing_log(os.path.splitext(parser.chain_file)[0] + '.log')
            except OSError:
                log.exception("Error during hosts dataset parsing -> commit")
                certdb.commit()
                raise DatasetParsingError("Error during hosts dataset parsing")
        # Remove parsed datasets
        for dataset in parsable:
            dataset.delete(DatasetState.COLLECTED)
        return tuple(parsable) if parsable else None

    def __validate(self, datasets: Tuple[Dataset], certdb: CertDB,
                   validator: object, validator_cfg: dict) -> Tuple[Dataset]:
        validatable = []
        for dataset in datasets:
            chain_file = dataset.full_path(DatasetState.PARSED, self._CHAINS_NAME_SUFFIX, True)
            if chain_file:
                log.info("Will validate dataset: %s", dataset.static_filename)
                for host, chain in RapidParser.read_chains(chain_file):
                    _ = validator(chain, validator_cfg)
                    print(host, chain)
                    certdb.export()
                validatable.append(dataset)

        return tuple(validatable) if validatable else None

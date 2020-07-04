"""This module contains DatasetManager interface implementation of RAPID dataset type."""

import os
import logging
from collections import OrderedDict
from datetime import datetime
from typing import Tuple, Type
from cevast.certdb import CertDB
from cevast.validation import CertValidator
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
    # TODO support port range in signle validate or parse tasks
    def __init__(self, repository: str, date: datetime.date = datetime.today().date(),
                 ports: Tuple[str] = ('443',), cpu_cores: int = 1):
        self._repository = repository
        self._date = date
        self._ports = (ports,) if isinstance(ports, str) else ports
        self._cpu_cores = cpu_cores
        self.__date_id = date.strftime('%Y%m%d')
        log.info('RapidDatasetManager initialized with repository=%s, date=%s, ports=%s', repository, date, ports)

    def run(self, task_pipline: Tuple[Tuple[DatasetManagerTask, dict]]) -> None:
        collected_datasets, parsed_datasets = None, None
        # Sort just to ensure valid sequence
        task_pipline = sorted(task_pipline, key=lambda x: x[0])
        log.info('Started with task pipeline %s', task_pipline)
        try:
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
                    raise NotImplementedError  # Not implemented yet

                # Runs parsing TASK, parsed datasets might be used in next task
                elif task == DatasetManagerTask.PARSE:
                    if collected_datasets:  # If some datasets were just collected in pipeline, use these
                        parsed_datasets = self.__parse(datasets=collected_datasets, **params)
                    else:
                        parsed_datasets = self.parse(**params)
                    log.info("Parsed datasets: %s", parsed_datasets)

                # Runs validation TASK
                elif task == DatasetManagerTask.VALIDATE:
                    if parsed_datasets:
                        validated_datasets = self.__validate(datasets=parsed_datasets, **params)
                    else:  # If some datasets were just parsed in pipeline, use these
                        validated_datasets = self.validate(**params)
                    log.info("Validated datasets: %s", validated_datasets)
        except TypeError:
            log.exception("Error when running task pipeline, are the arguments set correctly?")
        log.info("Finished")

    def collect(self, api_key: str = None) -> Tuple[Dataset]:
        log.info('Collecting started')
        collector = RapidCollector(api_key)
        # Create dummy dataset only to get target dir
        dummy_dataset = Dataset(self._repository, self.dataset_type, self.__date_id, None)
        download_dir = dummy_dataset.path(DatasetState.COLLECTED)
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
        datasets = self.__init_datasets()
        # Parse datasets
        parsed = self.__parse(certdb=certdb, datasets=datasets)
        return parsed if parsed else None

    def validate(self, validator: CertValidator, validator_cfg: dict) -> Tuple[Dataset]:
        datasets = self.__init_datasets()
        # Validate datasets
        validated = self.__validate(datasets=datasets, validator=validator, validator_cfg=validator_cfg)
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
        log.info('Parsing started')
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
        log.info('Parsing finished')
        return tuple(parsable) if parsable else None

    def __validate(self, datasets: Tuple[Dataset], validator: Type[CertValidator], validator_cfg: dict) -> Tuple[Dataset]:
        log.info('Validation started')
        validatable = []

        for dataset in datasets:
            chain_file = dataset.full_path(DatasetState.PARSED, self._CHAINS_NAME_SUFFIX, True)
            if chain_file:
                validatable.append(dataset)
                filename = os.path.join(dataset.path(DatasetState.VALIDATED), dataset.static_filename)
                # Open validator as context manager
                with validator(output_file=filename, processes=self._cpu_cores, **validator_cfg) as validator_ctx:
                    log.info("Will validate dataset: %s", dataset.static_filename)
                    for host, chain in RapidParser.read_chains(chain_file):
                        validator_ctx.schedule(host, chain)
                    # Indicate that no more validation data will be scheduled
                    validator_ctx.done()
                    log.info("Dataset validation finished")

        log.info('Validation finished')
        return tuple(validatable) if validatable else None

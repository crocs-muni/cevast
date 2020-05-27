"""This module contains parser implementation for Rapid dataset type"""

import os
import gzip
import logging
from contextlib import ExitStack
from cevast.dataset import DatasetType
from cevast.certdb import CertDB
from cevast.utils import BASE64_to_PEM

__author__ = 'Radim Podola'

log = logging.getLogger(__name__)

# TODO - common_pool/  - additional pool of common certificates downloaded separately from datasets
# -- use CertFileDB with 0 level hierarchy??


class RapidParser:
    """A parser class which parses RAPID dataset type."""

    dataset_type = DatasetType.RAPID

    # TODO design init and config properly
    # will receive Dataset instance
    def __init__(self, certs_dataset: str, hosts_dataset: str, dataset_id: str):
        # Check dataset files
        if not os.path.isfile(certs_dataset):
            raise FileNotFoundError(certs_dataset)
        if not os.path.isfile(hosts_dataset):
            raise FileNotFoundError(hosts_dataset)
        # Initialize parser
        log.info('Initializing parser for dataset %s (%s:%s)', dataset_id, certs_dataset, hosts_dataset)
        self.certs_dataset = certs_dataset
        self.hosts_dataset = hosts_dataset
        self.dataset_id = dataset_id
        # Initialize dataset parsing log
        self.__parsing_log = {
            'total_certs': 0,
            'total_hosts': 0,
            'total_host_certs': 0,
            'broken_chains': 0,
        }

    # a class method to create a RapidParser object from config file.
    @classmethod
    def from_config(cls, config):
        return cls(config['certs_dataset'], config['hosts_dataset'], config['dataset_id'])

    @staticmethod
    def read_certs(dataset: str) -> tuple:
        """
        Generator parsing certificates from dataset one by one.
        Tuple ('cert_id', 'certificate') is returned for each parsed certificated.
        """
        log.info('Start parsing certificates from dataset: %s', dataset)
        with gzip.open(dataset, 'rt') as r_file:
            for line in r_file:
                yield [x.strip() for x in line.split(',')]

    @staticmethod
    def read_chains(dataset: str) -> tuple:
        """
        Generator parsing host certificate chains from dataset one by one.
        Tuple ('host IP', [certificate chain]) is returned for each parsed certificated.
        """
        log.info('Start parsing host chains from dataset: %s', dataset)
        chain = []
        last = None
        with gzip.open(dataset, 'rt') as r_file:
            for line in r_file:
                curr, sha = [x.strip() for x in line.split(',')]

                if last and curr != last:
                    yield last, chain
                    chain.clear()
                # Building the chain
                chain.append(sha)
                last = curr
            yield last, chain

    def store_certs(self, certdb: CertDB) -> None:
        """Parses certificates from dataset and stores them into DB"""
        for sha, cert in self.read_certs(self.certs_dataset):
            certdb.insert(sha, BASE64_to_PEM(cert))
            self.__parsing_log['total_certs'] += 1

    def store_chains(self, certdb: CertDB, separate_broken_chains: bool = True) -> None:
        def write_chain(host: str, chain: list):
            self.__parsing_log['total_hosts'] += 1
            line = host + ',' + ','.join(chain) + '\n'
            if separate_broken_chains:
                # Try to find all the certificates in DB
                if certdb.exists_all(chain):
                    f_full_chains.write(line)
                else:
                    self.__parsing_log['broken_chains'] += 1
                    f_broken_chains.write(line)
            else:
                f_full_chains.write(line)

        if not separate_broken_chains:
            self.__parsing_log['broken_chains'] = -1

        with ExitStack() as stack:
            f_full_chains = stack.enter_context(gzip.open(self.dataset_id + ".gz", 'wt'))
            if separate_broken_chains:
                f_broken_chains = stack.enter_context(gzip.open(self.dataset_id + '_broken.gz', 'wt'))

            for host, chain in self.read_chains(self.hosts_dataset):
                self.__parsing_log['total_host_certs'] += 1
                # Writing chain
                write_chain(host, chain)

    @property
    def parsing_log(self):
        return self.__parsing_log

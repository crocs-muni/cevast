"""This module contains implementation of RAPID dataset type unifier."""

import os
import gzip
import logging
import json
from contextlib import ExitStack
from cevast.certdb import CertDB
from cevast.utils import BASE64_to_PEM
from ..dataset import DatasetType

__author__ = 'Radim Podola'

log = logging.getLogger(__name__)


class RapidUnifier:
    """
    A unifier class that unifies RAPID dataset.

    Class parses RAPID dataset, stores the parsed certificates into CertDB
    and builds certificate chains. Chains are written as unified dataset format.
    """

    dataset_type = DatasetType.RAPID

    def __init__(self, certs_dataset: str, hosts_dataset: str, chain_file: str, broken_chain_file: str = None):
        # Check dataset files
        if not os.path.isfile(certs_dataset):
            raise FileNotFoundError(certs_dataset)
        if not os.path.isfile(hosts_dataset):
            raise FileNotFoundError(hosts_dataset)
        # Initialize unifier
        log.info('Initializing unifier for dataset files (%s:%s)', certs_dataset, hosts_dataset)
        self._certs_dataset = certs_dataset
        self._hosts_dataset = hosts_dataset
        self._chain_file = chain_file
        self._broken_chain_file = broken_chain_file
        # Initialize dataset unification log
        self.__unification_log = {
            'total_certs': 0,
            'total_hosts': 0,
            'total_host_certs': 0,
            'broken_chains': 0,
        }

    @property
    def certs_dataset(self) -> str:
        """Getter property of certs dataset."""
        return self._certs_dataset

    @property
    def hosts_dataset(self) -> str:
        """Getter property of hosts dataset."""
        return self._hosts_dataset

    @property
    def chain_file(self) -> str:
        """Getter property of chain file."""
        return self._chain_file

    @property
    def unification_log(self) -> dict:
        """Getter property of unification log."""
        return self.__unification_log

    @staticmethod
    def parse_certs(dataset: str) -> tuple:
        """
        Generator parsing certificates from dataset one by one.
        Tuple ('cert_id', 'certificate') is returned for each parsed certificated.
        """
        log.info('Start parsing certificates from dataset: %s', dataset)
        with gzip.open(dataset, 'rt') as r_file:
            for line in r_file:
                yield [x.strip() for x in line.split(',')]

    @staticmethod
    def parse_chains(dataset: str) -> tuple:
        """
        Generator parsing host certificate records and building chains from dataset one by one.
        Tuple ('host IP', [certificate chain]) is returned for each parsed host IP.
        """
        log.info('Start parsing and building host chains from dataset: %s', dataset)
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

    # TODO this one might be generic
    @staticmethod
    def read_chains(dataset: str) -> tuple:
        """
        Generator reading certificate chains from unified dataset one by one.
        Tuple ('host IP', [certificate chain]) is returned for each host.
        """
        log.info('Start reading certificate chains from dataset: %s', dataset)
        with gzip.open(dataset, 'rt') as r_file:
            for line in r_file:
                read_line = line.strip().split(',')
                yield read_line[0], read_line[1:]

    def store_certs(self, certdb: CertDB) -> None:
        """Parses certificates from dataset and stores them into CertDB."""
        for sha, cert in self.parse_certs(self._certs_dataset):
            certdb.insert(sha, BASE64_to_PEM(cert))
            self.__unification_log['total_certs'] += 1

    def store_chains(self, certdb: CertDB) -> None:
        """
        Parses and builds certificate chains from dataset and stores them into the unified `chain_file` file.

        If `broken_chain_file` is provided, the chains that are not available (in the dataset nor the CertDB)
        are stored into this separate file.
        """

        def write_chain(host: str, chain: list):
            self.__unification_log['total_hosts'] += 1
            line = host + ',' + ','.join(chain) + '\n'
            if self._broken_chain_file:
                # Try to find all the certificates in DB
                if certdb.exists_all(chain):
                    f_full_chains.write(line)
                else:
                    self.__unification_log['broken_chains'] += 1
                    f_broken_chains.write(line)
            else:
                f_full_chains.write(line)

        if not self._broken_chain_file:
            self.__unification_log['broken_chains'] = -1

        with ExitStack() as stack:
            f_full_chains = stack.enter_context(gzip.open(self._chain_file, 'wt'))
            if self._broken_chain_file:
                f_broken_chains = stack.enter_context(gzip.open(self._broken_chain_file, 'wt'))

            for host, chain in self.parse_chains(self._hosts_dataset):
                self.__unification_log['total_host_certs'] += len(chain)
                # Writing chain
                write_chain(host, chain)

    def save_unification_log(self, filename: str) -> None:
        """Save unification log to filename."""
        log_str = json.dumps(self.unification_log, sort_keys=True, indent=4)
        log.info('Saving unification log: %s', filename)
        with open(filename, 'w') as outfile:
            outfile.write(log_str)

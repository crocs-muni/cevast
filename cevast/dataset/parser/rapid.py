"""This module contains parser implementation for Rapid dataset type"""

import os
import json
import gzip
import logging
from contextlib import ExitStack
from cevast.dataset.dataset_manager import DatasetType
from cevast.certdb import CertDB
from cevast.utils import BASE64_to_PEM

__author__ = 'Radim Podola'

log = logging.getLogger('cevast.RapidParser')

# TODO - common_pool/  - additional pool of common certificates downloaded separately from datasets
# -- use CertFileDB with 0 level hierarchy??
# TODO make ABC parser


class RapidParser:
    """A parser class which parses RAPID dataset type."""

    dataset_type = DatasetType.RAPID

    # TODO design init and config properly
    def __init__(self, dataset_certs: str, dataset_hosts: str, dataset_id: str):
        self.dataset_certs = dataset_certs
        self.dataset_hosts = dataset_hosts
        self.dataset_id = dataset_id
        log.info('Initializing parser for dataset %s (%s:%s)', dataset_id, dataset_certs, dataset_hosts)
        # Check dataset files
        if not os.path.isfile(self.dataset_certs):
            raise FileNotFoundError(self.dataset_certs)
        if not os.path.isfile(self.dataset_hosts):
            raise FileNotFoundError(self.dataset_hosts)

    # a class method to create a RapidParser object from config file.
    @classmethod
    def from_config(cls, config):
        return cls(config['dataset_certs'], config['dataset_hosts'], config['dataset_id'])

    def parse(self, certdb: CertDB, separate_broken_chains: bool = True) -> bool:
        def write_chain(host: str, chain: list):
            dataset_meta['total_hosts'] += 1
            line = host + ',' + ','.join(chain) + '\n'
            if separate_broken_chains:
                # Compute difference
                diff = set(chain) - available_certs
                # Try to find the remaining certs in DB
                if not diff or certdb.exists_all(list(diff)):
                    f_full_chains.write(line)
                else:
                    dataset_meta['broken_chains'] += 1
                    f_broken_chains.write(line)
            else:
                f_full_chains.write(line)

        # During parsing some dataset metadata are collected
        dataset_meta = dataset_meta = {
            'total_certs': 0,
            'total_hosts': 0,
            'total_host_certs': 0,
            'broken_chains': 0 if separate_broken_chains else -1,
        }
        available_certs = set()

        # Parse certificates from dataset
        log.info('Start parsing certificates from dataset: %s', self.dataset_certs)
        try:
            with gzip.open(self.dataset_certs, 'rt') as r_file:
                for line in r_file:
                    dataset_meta['total_certs'] += 1
                    sha, cert = self.parse_certs_line(line)

                    certdb.insert(sha, BASE64_to_PEM(cert))
                    # Track managed certs for broken chain separation
                    if separate_broken_chains:
                        available_certs.add(sha)
        except (OSError, ValueError):
            log.exception("Fatal error during parsing certificates -> rollback and return")
            certdb.rollback()
            return False

        # Parse host scans from dataset
        log.info('Start parsing host scans from dataset: %s', self.dataset_hosts)
        chain = []
        last = None
        try:
            with ExitStack() as stack:
                r_file = stack.enter_context(gzip.open(self.dataset_hosts, 'rt'))
                f_full_chains = stack.enter_context(gzip.open(self.dataset_id + ".gz", 'wt'))
                if separate_broken_chains:
                    f_broken_chains = stack.enter_context(gzip.open(self.dataset_id + '_broken.gz', 'wt'))

                for line in r_file:
                    dataset_meta['total_host_certs'] += 1
                    curr, sha = self.parse_hosts_line(line)

                    if last and curr != last:
                        # Writing the chain
                        write_chain(last, chain)
                        chain.clear()
                    # Building the chain
                    chain.append(sha)
                    last = curr
                # Writing last chain
                write_chain(last, chain)
        except OSError:
            log.exception("Fatal error during parsing host scans -> commit and return")
            certdb.commit()
            return False

        # Commit inserted certificates now
        # certdb.exists_all is faster before commit
        certdb.commit()
        # Store dataset metadata
        meta_filename = self.dataset_id + '.json'
        meta_str = json.dumps(dataset_meta, sort_keys=True, indent=4)
        log.info('Storing metadata file about dataset: %s', meta_filename)
        log.debug(meta_str)
        with open(meta_filename, 'w') as outfile:
            outfile.write(meta_str)

        return True

    @staticmethod
    def parse_hosts_line(line):
        return [x.strip() for x in line.split(',')]

    @staticmethod
    def parse_certs_line(line):
        return [x.strip() for x in line.split(',')]

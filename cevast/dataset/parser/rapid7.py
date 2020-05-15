"""This module contains parser implementation for Rapid dataset type"""

import logging
import gzip
import io
import json
from contextlib import ExitStack
from cevast.dataset.dataset_manager import DatasetType
from cevast.certdb import CertDB
from cevast.utils import BASE64_to_PEM

__author__ = 'Radim Podola'

logger = logging.getLogger(__name__)


# TODO here   - common_pool/       - additional pool of common certificates downloaded separately from datasets

class RapidParser:

    dataset_type = DatasetType.RAPID

    # TODO design init and config properly
    def __init__(self, dataset_certs, dataset_hosts, dataset_id):
        self.dataset_certs = dataset_certs
        self.dataset_hosts = dataset_hosts
        self.dataset_id = dataset_id
        logger.info('I am RapidParser: {}:{}:{}'.format(dataset_id, dataset_certs, dataset_hosts))

    # a class method to create a RapidParser object from config file.
    @classmethod
    def fromConfig(cls, config):
        return cls(config['dataset_certs'],
                   config['dataset_hosts'],
                   config['dataset_id'])

    def parse(self, certdb: CertDB, separate_broken_chains=True):
        def write_chain(ip: str, chain: list):
            dataset_meta['total_hosts'] += 1
            if separate_broken_chains:
                # Compute difference
                diff = set(chain) - available_certs
                # Try to find the remaining certs in DB
                if not diff or certdb.exists_all(list(diff)):
                    f_full_chains.write(ip + ',' + ','.join(shas))
                else:
                    dataset_meta['broken_chains'] += 1
                    f_broken_chains.write(ip + ',' + ','.join(shas))
            else:
                f_full_chains.write(ip + ',' + ','.join(shas))

        # During parsing some dataset metadata are collected
        dataset_meta = {'total_certs': 0,
                        'total_hosts': 0,
                        'total_host_certs': 0,
                        'broken_chains': 0 if separate_broken_chains else -1}
        available_certs = set()

        # TODO try catch -> rollback ?

        # Parse certificates from dataset
        logger.info('Start parsing certificates from dataset: {}'.format(self.dataset_certs))
        with gzip.open(self.dataset_certs, 'rt') as r_file:
            for line in r_file:
                dataset_meta['total_certs'] += 1
                sha, cert = self.parse_certs_line(line)

                certdb.insert(sha, BASE64_to_PEM(cert))
                # Track managed certs for broken chain separation
                if separate_broken_chains:
                    available_certs.add(sha)

        # Parse host scans from dataset
        logger.info('Start parsing host scans from dataset: {}'.format(self.dataset_hosts))
        chain = []
        last = None
        with ExitStack() as stack:
            r_file = stack.enter_context(gzip.open(self.dataset_hosts, 'rt', ))
            f_full_chains = stack.enter_context(gzip.open(self.dataset_id + ".gz", 'wt'))
            if separate_broken_chains:
                f_broken_chains = stack.enter_context(gzip.open(self.dataset_id + '_broken.gz', 'wt'))

            for line in r_file:
                dataset_meta['total_host_certs'] += 1
                ip, sha = self.parse_hosts_line(line)

                if last and ip != last:
                    # Writing the chain
                    write_chain(ip, chain)
                    chain.clear()
                # Building the chain
                chain.append(sha)
                last = ip
            # Writing last chain
            write_chain(ip, chain)

        # Commit inserted certificates now
        # certdb.exists_all is faster before commit
        certdb.commit()

        # Store dataset metadata
        meta_filename = self.dataset_id + '.json'
        meta_str = json.dumps(dataset_meta, sort_keys=True, indent=4))
        logger.info('Storing metadata file about dataset: {}'.format(meta_filename))
        logger.debug(meta_str)
        with open(meta_filename, 'w') as outfile:
            outfile.write(meta_str)

    @staticmethod
    def parse_hosts_line(line):
        return [x.strip() for x in line.split(',')]

    @staticmethod
    def parse_certs_line(line):
        return [x.strip() for x in line.split(',')]

"""This module contains parser implementation for Rapid dataset type"""

import logging
import gzip
import io
from sortedcontainers import SortedList
from cevast.dataset.dataset_manager import DatasetType
from cevast.certdb import CertFileDB

__author__ = 'Radim Podola'

logger = logging.getLogger(__name__)


class RapidParser:

    dataset_type = DatasetType.RAPID

    def __init__(self, dataset_certs, dataset_hosts, target_folder):
        self.dataset_certs = dataset_certs
        self.dataset_hosts = dataset_hosts
        self.target_folder = target_folder
        logger.info('I am RapidParser: {}:{}'.format(dataset_certs, dataset_hosts))

    # a class method to create a RapidParser object from config file.
    @classmethod
    def fromConfig(cls, config):
        return cls(config['dataset_certs'],
                   config['dataset_hosts'],
                   config['target_folder'])

    def parse(self, cert_db: CertFileDB, separate_broken_chains=True):
        logger.info('Start parsing hosts dataset: {}'.format(self.dataset_hosts))

        # dict to keep tracking unique servers found in hosts file
        hosts = {}
        # SortedList to keep tracking of probably non-server certificates found in hosts file
        probably_non_server_certs_sl = SortedList()

        available_certs_sl = SortedList()

        with gzip.open(self.dataset_hosts, 'rt') as r_file:
            f = io.BufferedReader(r_file)
            for line in f:
                ip, sha = self.parse_hosts_line(line)

                if ip in hosts:
                    # building chain
                    hosts[ip].append(sha)

                    if sha not in probably_non_server_certs_sl:
                        probably_non_server_certs_sl.add(sha)
                else:
                    hosts[ip] = [sha]

        logger.info('Start parsing certs dataset: {}'.format(self.dataset_certs))

        with gzip.open(self.dataset_certs, 'rt') as r_file:
            f = io.BufferedReader(r_file)
            for line in f:
                sha, cert = self.parse_certs_line(line)
                available_certs_sl.add(sha)

                cert_db.insert(sha, cert, sha in probably_non_server_certs_sl)

        logger.info('Writing metadata files about dataset')
        logger.debug('{} hosts entries parsed'.format(len(hosts)))

        chains_file = gzip.GzipFile(self.target_folder + ".gz", 'wb')
        broken_chains_file = gzip.GzipFile(self.target_folder + "_broken.gz", 'wb')

        for ip, shas in hosts.items():
            if cert_db.exists_all(shas, False):
                chains_file.write(bytes(ip + ',' + ','.join(shas) + '\n', encoding='utf8'))
            else:
                broken_chains_file.write(bytes(ip + ',' + ','.join(shas) + '\n', encoding='utf8'))

        chains_file.close()
        broken_chains_file.close()

        cert_db.commit()

    @staticmethod
    def parse_hosts_line(line):
        return [x.strip() for x in line.decode('utf-8').split(',')]

    @staticmethod
    def parse_certs_line(line):
        return [x.strip() for x in line.decode('utf-8').split(',')]

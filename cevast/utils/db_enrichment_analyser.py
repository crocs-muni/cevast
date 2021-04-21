"""
This module generates database enrichment stats - numbers of complete chains in a dataset
with given numbers of most common missing certificates added to DB.

output: <enrichment level>,<number of complete chains for that particular level>
"""

import logging
import os
from ..dataset.unifiers.rapid import RapidUnifier

__author__ = 'Róbert Šuška'

log = logging.getLogger(__name__)
cli_log = logging.getLogger('CEVAST_CLI')


class EnrichmentAnalyser:
    def __init__(self, certs_file, hosts_file, enrichment_depth):
        self.__certs_file = certs_file
        self.__hosts_file = hosts_file

        self.__enrichment_depth = enrichment_depth

        self.__cert_hashes = set()
        self.__missing_cert_counts = {}
        self.__sorted_missing_certs = []
        self.__broken_chain_counts_with_enrichments = {}
        self.__chain_count = 0

    def __parse_certs_file(self):
        cli_log.info("Parsing certs...")

        self.__cert_hashes.clear()

        for cert_hash, _ in RapidUnifier.parse_certs(self.__certs_file):
            self.__cert_hashes.add(cert_hash)

    def __count_missing_certs_in_chains(self):
        cli_log.info('Counting missing certs in chains...')

        self.__missing_cert_counts.clear()

        for _, cert_hash_chain in RapidUnifier.parse_chains(self.__hosts_file):
            for cert_hash in cert_hash_chain:
                if cert_hash not in self.__cert_hashes:
                    if cert_hash not in self.__missing_cert_counts:
                        self.__missing_cert_counts[cert_hash] = 1
                    else:
                        self.__missing_cert_counts[cert_hash] += 1

        cli_log.info('Sorting missing certs...')

        self.__sorted_missing_certs = [cert_hash for cert_hash, count in sorted(self.__missing_cert_counts.items(), key=lambda item: item[1], reverse=True)]

    def __count_broken_chains_with_enrichments(self):
        cli_log.info('Counting broken chains with enrichments...')

        self.__broken_chain_counts_with_enrichments.clear()
        self.__chain_count = 0

        for _, cert_hash_chain in RapidUnifier.parse_chains(self.__hosts_file):
            self.__determine_chain_completeness_with_enrichments(cert_hash_chain)
            self.__chain_count += 1

    def __determine_chain_completeness_with_enrichments(self, cert_hash_chain):
        missing_certs = []

        for cert_hash in cert_hash_chain:
            if cert_hash not in self.__cert_hashes:
                missing_certs.append(cert_hash)

        if len(missing_certs) > 0:
            if 0 not in self.__broken_chain_counts_with_enrichments:
                self.__broken_chain_counts_with_enrichments[0] = 1
            else:
                self.__broken_chain_counts_with_enrichments[0] += 1

            for i in range(1, min(self.__enrichment_depth + 1, len(self.__sorted_missing_certs) + 1)):
                if not all(missing_cert in self.__sorted_missing_certs[:i] for missing_cert in missing_certs):
                    if i not in self.__broken_chain_counts_with_enrichments:
                        self.__broken_chain_counts_with_enrichments[i] = 1
                    else:
                        self.__broken_chain_counts_with_enrichments[i] += 1
                else:
                    break

    def __write_results(self):
        results_file_name = '{0}_enrichment_stats'.format('_'.join(os.path.basename(self.__certs_file).split('_')[:2]))

        cli_log.info('Writing results into \"{0}\"'.format(results_file_name))

        with open(results_file_name, 'w') as results_file:
            for cert_hash in self.__sorted_missing_certs[:self.__enrichment_depth]:
                results_file.write('{0}\n'.format(cert_hash))

            cli_log.info('\nTotal chains: {0}\n'.format(self.__chain_count))
            results_file.write('\nTotal chains: {0}\n\n'.format(self.__chain_count))

            for enrichment in sorted(self.__broken_chain_counts_with_enrichments):
                complete_chain_count = self.__chain_count - self.__broken_chain_counts_with_enrichments[enrichment]

                cli_log.info('Enrichment {0}: {1}% complete ({2})'.format(enrichment, round(complete_chain_count / self.__chain_count, 4), complete_chain_count))
                results_file.write('{0},{1}\n'.format(enrichment, complete_chain_count))

    def run(self):
        log.info('EnrichmentAnalyser start')

        self.__parse_certs_file()
        self.__count_missing_certs_in_chains()
        self.__count_broken_chains_with_enrichments()
        self.__write_results()

        log.info('EnrichmentAnalyser done')


"""This module contains implementation of RAPID dataset type analyzer."""
# pylint: skip-file
# flake8: noqa
import gzip
import io
import sys
from sortedcontainers import SortedList
import statistics
import re
import os
import gzip
import logging
from contextlib import ExitStack
from cevast.dataset import DatasetType
from cevast.certdb import CertDB
from cevast.utils import BASE64_to_PEM

__author__ = 'Radim Podola'

log = logging.getLogger(__name__)


class RapidAnalyzer:
    """A analyzer class of RAPID dataset type."""
"""
    #SortedList to manage certificates IDs of all datasets
    all_certificates = SortedList()

    for ds_key, ds_value in ((k, v["certs_file"]) for k, v in datasets.items() if "certs_file" in v):
        print("Parsing certificates from dataset: {}".format(ds_key))

        with gzip.open(ds_value, 'rb') as r_file:
            f = io.BufferedReader(r_file)
            for line in f:
                sha = line.decode('utf-8').split(',')[0]

                if sha not in all_certificates:
                    all_certificates.add(sha)

    print("[{}] certificates managed: {}".format(ds_value, len(all_certificates)))

    for ds_key, ds_value in ((k, v["hosts_file"]) for k, v in datasets.items() if "hosts_file" in v):
        print("----------------------------------------------------------")
        print("Analyzing certificates from dataset: {}".format(ds_key))

        #dict to keep tracking amount of certificates found in hosts file
        host_certs_cntr = {}
        #dict to keep tracking unique servers found in hosts file
        hosts = {}
        #counter -> amount of certificates provided by certs files
        provided_cntr = 0
        chain_broken = 0
        #SortedList to keep tracking of probably non-server certificates found in hosts file
        probably_non_server_certs_sl = SortedList()

        with gzip.open(ds_value, 'rb') as r_file:
            f = io.BufferedReader(r_file)
            for line in f:
                ip, sha = line.decode('utf-8').split(',')
                sha = sha.strip()

                included = True if sha in all_certificates else False

                # if cert is already analyzed, increment occurence
                if sha in host_certs_cntr:
                    host_certs_cntr[sha] += 1
                else:
                    host_certs_cntr[sha] = 1
                    if included:
                        provided_cntr += 1

                if ip in hosts:
                    if hosts[ip] and not included:
                        hosts[ip] = False
                        chain_broken += 1

                    if sha not in probably_non_server_certs_sl:
                        probably_non_server_certs_sl.add(sha)

                else:
                    hosts[ip] = included
                    if not included:
                        chain_broken += 1

        # PRINT statistics
        print("Servers analyzed:      ", len(hosts))
        print("Certificates analyzed: ", len(host_certs_cntr))
        print("Probably non-server certificates: ", len(probably_non_server_certs_sl))
        print("")
        print("Certificates provided in datasets:     ", provided_cntr)
        print("Certificates NOT provided in datasets: ", len(host_certs_cntr) - provided_cntr)
        print("Chains NOT able to build: ", chain_broken)
        print("")
        st_values = host_certs_cntr.values()
        print("Median:", statistics.median(st_values))
        print("Mean:", statistics.mean(st_values))"""
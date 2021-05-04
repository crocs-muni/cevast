#!/usr/bin/python3

"""
This module contains an implementation of a certificate chain validation client for command-line OpenSSL.

The validation client can be both imported and used externally (as a standalone module) through the provided
command-line interface.

..Important::
  The validation client must be set up correctly before use. It is necessary to ensure, that OpenSSL is installed
  correctly, and that `TRUST_STORE_FILE` is set to the path to the local trust store.

  When using the validation client externally, these prerequisites are always checked before validation is performed.
  When the validation client is imported, it is necessary to do these checks manually (i.e., using the method `is_setup_correctly()`).
"""

import argparse
import datetime
import os
import re
import subprocess


# noinspection PyBroadException
class Openssl:
    """
    A class for certificate chain validation client utilizing command-line OpenSSL.

    Special error codes:

    -1: any error / exception not related to the certificate chain validation (algorithm) itself
    """

    TRUST_STORE_FILE = "/etc/pki/tls/cert.pem"

    @staticmethod
    def verify(chain, reference_time=None, crls=None, **kwargs):
        """
        Validates a certificate chain.

        `chain` is a list of paths to certificates forming a chain.
        `reference_time` is a reference time of validation in seconds since the epoch.
        `crls` is a list of paths to CRLs.
        `kwargs` are other, unexpected arguments.

        The returned result is a list of a set of error codes returned by command-line OpenSSL.
        """

        chain = list(chain)

        try:
            command = ["openssl", "verify", "-CAfile", Openssl.TRUST_STORE_FILE, "-no-CApath"]

            if reference_time:
                command += ["-attime", str(reference_time)]

            if crls:
                for crl in crls:
                    command += ["-CRLfile", crl]

                command += ["-crl_check"]

            command += [element for intermediate in reversed(chain[1:]) for element in ["-untrusted", intermediate]] if len(chain) > 1 else []
            command += [chain[0]]

            result = [0]

            try:
                subprocess.check_output(command, stderr=subprocess.STDOUT)
            except subprocess.CalledProcessError as error:
                result = [-1]

                matches = re.findall(r'\nerror (\d+) at', error.output.decode())

                if len(matches) > 0:
                    result = sorted(set([int(match) for match in matches]))
        except Exception:
            result = [-1]

        return result

    @staticmethod
    def is_setup_correctly():
        """
        Verifies that the validation client is set up correctly. (OpenSSL is installed and trust store exists)
        """

        is_setup_correctly = True

        try:
            subprocess.check_output(" ".join(["openssl", "version"]), stderr=subprocess.DEVNULL, shell=True)
        except Exception:
            is_setup_correctly = False

        if is_setup_correctly:
            if not os.path.isfile(Openssl.TRUST_STORE_FILE):
                is_setup_correctly = False

        return is_setup_correctly


if __name__ == "__main__":
    argument_parser = argparse.ArgumentParser(description="chain format: ENDPOINT [INTERMEDIATE ...] [CA]")

    argument_parser.add_argument("-r", type=lambda s: int(datetime.datetime.strptime(s, "%Y-%m-%d").timestamp()),
                                 required=False, dest="reference_date_as_time", metavar="DATE",
                                 help="reference date in format YYYY-MM-DD (at 00:00:00)")
    argument_parser.add_argument("-t", type=int, required=False, dest="reference_time", metavar="N",
                                 help="reference time in seconds since the epoch (surpassing reference date)")
    argument_parser.add_argument("--crl", type=str, action="append", required=False, dest="crls", metavar="FILE",
                                 help="certificate revocation list (can be used multiple times)")
    argument_parser.add_argument("CERTIFICATE", type=str, nargs="+")

    if Openssl.is_setup_correctly():
        args = argument_parser.parse_args()
        print(Openssl.verify(args.CERTIFICATE, reference_time=args.reference_time if args.reference_time is not None else args.reference_date_as_time, crls=args.crls))
    else:
        print("Client is not set up correctly")

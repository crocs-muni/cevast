#!/usr/bin/python3

"""
This module contains an implementation of a certificate chain validation client for Botan.

The validation client can be both imported and used externally (as a standalone module) through the provided
command-line interface.

..Important::
  The validation client must be set up correctly before use. It is necessary to ensure, that `TRUST_STORE_DIRECTORY` is set to the path to the local trust store.

  When using the validation client externally, these prerequisites are always checked before validation is performed.
  When the validation client is imported, it is necessary to do these checks manually (i.e., using the method `is_setup_correctly()`).
"""

import argparse
import datetime
import contextlib
import io
import os
import botan2 as botan  # pylint: disable=import-error


# noinspection PyBroadException
class Botan:
    """
    A class for certificate chain validation client utilizing Botan.

    Special error codes:

    -1: any error / exception not related to the certificate chain validation (algorithm) itself
    """

    TRUST_STORE_DIRECTORY = "/etc/pki/ca-trust/extracted/pem/"

    @staticmethod
    def verify(chain, reference_time=None, crls=None, **kwargs):
        """
        Validates a certificate chain.

        `chain` is a list of paths to certificates forming a chain.
        `reference_time` is a reference time of validation in seconds since the epoch.
        `crls` is a list of paths to CRLs.
        `kwargs` are other, unexpected arguments.

        The returned result is a list containing a single error code returned by Botan.
        """

        with contextlib.redirect_stderr(io.StringIO()):
            chain = list(chain)

            try:
                server = botan.X509Cert(**{"filename": chain[0]})
                intermediates = [botan.X509Cert(**{"filename": i}) for i in chain[1:]] if len(chain) > 1 else None

                result = int(server.verify(intermediates=intermediates,
                                           trusted_path=Botan.TRUST_STORE_DIRECTORY,
                                           reference_time=reference_time if reference_time else 0,
                                           crls=[botan.X509CRL(**{"filename": crl}) for crl in crls] if crls else None))
            except Exception:
                result = -1

            return [result]

    @staticmethod
    def is_setup_correctly():
        """
        Verifies that the validation client is set up correctly. (trust store exists)
        """

        is_setup_correctly = True

        if not os.path.isdir(Botan.TRUST_STORE_DIRECTORY):
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

    if Botan.is_setup_correctly():
        args = argument_parser.parse_args()
        print(Botan.verify(args.CERTIFICATE, reference_time=args.reference_time if args.reference_time is not None else args.reference_date_as_time, crls=args.crls))
    else:
        print("Client is not set up correctly")

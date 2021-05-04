#!/usr/bin/python3

"""
This module contains an implementation of a certificate chain validation client for pyOpenSSL --- OpenSSL's Python binding.

The validation client can be both imported and used externally (as a standalone module) through the provided
command-line interface.

..Important::
  The validation client must be set up correctly before use. It is necessary to ensure, that `TRUST_STORE_FILE` is set
  to the path to the local trust store.

  When using the validation client externally, these prerequisites are always checked before validation is performed.
  When the validation client is imported, it is necessary to do these checks manually (i.e., using the method `is_setup_correctly()`).
"""

import argparse
import datetime
import os
from OpenSSL import crypto


# noinspection PyBroadException
class Pyopenssl:
    """
    A class for certificate chain validation client utilizing pyOpenSSL.

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

        The returned result is a list containing a single error code returned by pyOpenSSL.
        """

        chain = list(chain)

        try:
            for i, certificate_path in enumerate(chain):
                with open(certificate_path) as input_file:
                    chain[i] = input_file.read().encode()

            store = crypto.X509Store()
            intermediates = []

            store.load_locations(Pyopenssl.TRUST_STORE_FILE)

            endpoint = crypto.load_certificate(crypto.FILETYPE_PEM, chain[0])

            if len(chain) > 1:
                for certificate_content in chain[1:]:
                    intermediates.append(crypto.load_certificate(crypto.FILETYPE_PEM, certificate_content))

            if reference_time:
                store.set_time(datetime.datetime.fromtimestamp(reference_time))

            if crls:
                for crl in crls:
                    with open(crl) as input_file:
                        store.add_crl(crypto.load_crl(type=crypto.FILETYPE_PEM, buffer="".join(input_file.readlines())))

                store.set_flags(crypto.X509StoreFlags.CRL_CHECK)

            result = 0

            try:
                crypto.X509StoreContext(store=store, certificate=endpoint,
                                        chain=intermediates if len(intermediates) > 0 else None).verify_certificate()
            except crypto.X509StoreContextError as error:
                result = int(error.args[0][0])
        except Exception:
            result = -1

        return [result]

    @staticmethod
    def is_setup_correctly():
        """
        Verifies that the validation client is set up correctly. (trust store exists)
        """

        is_setup_correctly = True

        if not os.path.isfile(Pyopenssl.TRUST_STORE_FILE):
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

    if Pyopenssl.is_setup_correctly():
        args = argument_parser.parse_args()
        print(Pyopenssl.verify(args.CERTIFICATE, reference_time=args.reference_time if args.reference_time is not None else args.reference_date_as_time, crls=args.crls))
    else:
        print("Client is not set up correctly")

#!/usr/bin/python3

import argparse
import datetime
import os
from OpenSSL import crypto


# noinspection PyBroadException
class Pyopenssl:
    TRUST_STORE_FILE = "/etc/pki/tls/cert.pem"

    @staticmethod
    def verify(chain, reference_time=None, crls=None, **kwargs):
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
        is_setup_correctly = True

        if not os.path.isfile(Pyopenssl.TRUST_STORE_FILE):
            is_setup_correctly = False

        return is_setup_correctly


if __name__ == "__main__":
    argument_parser = argparse.ArgumentParser(description="chain format: ENDPOINT [INTERMEDIATE ...] [CA]")

    argument_parser.add_argument("-r", type=lambda s: int(datetime.datetime.strptime(s, "%Y-%m-%d").timestamp()),
                                 required=False, default=int(datetime.datetime.today().date().strftime("%s")),
                                 dest="reference_time", metavar="DATE",
                                 help="reference date in format YYYY-MM-DD (Default is today)")
    argument_parser.add_argument("--crl", type=str, action="append", required=False, dest="crls", metavar="FILE",
                                 help="certificate revocation list (can be used multiple times)")
    argument_parser.add_argument("CERTIFICATE", type=str, nargs="+")

    if Pyopenssl.is_setup_correctly():
        args = argument_parser.parse_args()
        print(Pyopenssl.verify(args.CERTIFICATE, reference_time=args.reference_time, crls=args.crls))
    else:
        print("Client is not set up correctly")

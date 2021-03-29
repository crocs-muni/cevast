#!/usr/bin/python3

import argparse
from datetime import datetime
from OpenSSL import crypto


class Pyopenssl:
    TRUST_STORE_FILE = "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem"

    @staticmethod
    def verify(chain, load_from_disk=True, reference_time=None, crls=None):
        chain = list(chain)

        try:
            if load_from_disk:
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
                store.set_time(datetime.fromtimestamp(reference_time))

            if crls:
                for crl in crls:
                    with open(crl) as input_file:
                        store.add_crl(crypto.load_crl(type=crypto.FILETYPE_PEM, buffer="".join(input_file.readlines())))

                store.set_flags(crypto.X509StoreFlags.CRL_CHECK)

            code = 0

            try:
                crypto.X509StoreContext(store=store, certificate=endpoint,
                                        chain=intermediates if len(intermediates) > 0 else None).verify_certificate()
            except crypto.X509StoreContextError as error:
                code = int(error.args[0][0])
        except Exception:
            code = -1

        return str(code)


if __name__ == "__main__":
    argument_parser = argparse.ArgumentParser(description="chain format: ENDPOINT [INTERMEDIATE ...] [CA]")

    argument_parser.add_argument("-t", type=int, required=False, dest="reference_time", metavar="N",
                                 help="reference time (in seconds since the epoch)")
    argument_parser.add_argument("--crl", type=str, action="append", required=False, dest="crls", metavar="FILE",
                                 help="certificate revocation list (can be used multiple times)")
    argument_parser.add_argument("CERTIFICATE", type=str, nargs="+")

    args = argument_parser.parse_args()

    print(Pyopenssl.verify(args.CERTIFICATE, reference_time=args.reference_time, crls=args.crls))

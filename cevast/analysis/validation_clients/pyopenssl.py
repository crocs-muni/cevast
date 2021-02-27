#!/usr/bin/python3

import argparse
from OpenSSL import crypto
from datetime import datetime


def verify(chain, loadFromDisk=True, referenceTime=None, crls=None):
    chain = list(chain)

    try:
        if loadFromDisk:
            for i, certificatePath in enumerate(chain):
                with open(certificatePath) as inputFile:
                    chain[i] = inputFile.read().encode()

        store = crypto.X509Store()
        intermediates = []

        store.load_locations("/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem")

        endpoint = crypto.load_certificate(crypto.FILETYPE_PEM, chain[0])

        if len(chain) > 1:
            for certificateContent in chain[1:]:
                intermediates.append(crypto.load_certificate(crypto.FILETYPE_PEM, certificateContent))

        if referenceTime:
            store.set_time(datetime.fromtimestamp(referenceTime))

        if crls:
            for crl in crls:
                with open(crl) as inputFile:
                    store.add_crl(crypto.load_crl(type=crypto.FILETYPE_PEM, buffer="".join(inputFile.readlines())))

            store.set_flags(crypto.X509StoreFlags.CRL_CHECK)

        code = 0

        try:
            crypto.X509StoreContext(store=store, certificate=endpoint,
                                    chain=intermediates if len(intermediates) > 0 else None).verify_certificate()
        except crypto.X509StoreContextError as e:
            code = int(e.args[0][0])
    except Exception:
        code = -1

    return str(code)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="chain format: ENDPOINT [INTERMEDIATE ...] [CA]")
    parser.add_argument("-t", type=int, required=False, dest="referenceTime", metavar="N", help="reference time (in seconds since the epoch)")
    parser.add_argument("--crl", type=str, action="append", required=False, dest="crls", metavar="FILE", help="certificate revocation list (can be used multiple times)")
    parser.add_argument("CERTIFICATE", type=str, nargs="+")

    args = parser.parse_args()

    print(verify(args.CERTIFICATE, loadFromDisk=True, referenceTime=args.referenceTime, crls=args.crls))

#!/usr/bin/python3

import botan2 as botan
import argparse


# noinspection PyBroadException,PyUnresolvedReferences
def verify(chain, loadFromDisk=True, referenceTime=None, crls=None):
    chain = list(chain)

    try:
        server = botan.X509Cert(**{"filename" if loadFromDisk else "buf": chain[0]})
        intermediates = [botan.X509Cert(**{"filename" if loadFromDisk else "buf": i}) for i in chain[1:]] if len(chain) > 1 else None

        code = server.verify(intermediates=intermediates,
                             trusted_path="/etc/pki/ca-trust/extracted/pem/",
                             reference_time=referenceTime if referenceTime else 0,
                             crls=[botan.X509CRL(**{"filename" if loadFromDisk else "buf": crl}) for crl in crls] if crls else None)
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

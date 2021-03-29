#!/usr/bin/python3

import argparse
import botan2 as botan


class Botan:
    TRUST_STORE_DIRECTORY = "/etc/pki/ca-trust/extracted/pem/"

    @staticmethod
    def verify(chain, load_from_disk=True, reference_time=None, crls=None):
        chain = list(chain)

        try:
            server = botan.X509Cert(**{"filename" if load_from_disk else "buf": chain[0]})
            intermediates = [botan.X509Cert(**{"filename" if load_from_disk else "buf": i}) for i in chain[1:]] if len(chain) > 1 else None

            code = server.verify(intermediates=intermediates,
                                 trusted_path=Botan.TRUST_STORE_DIRECTORY,
                                 reference_time=reference_time if reference_time else 0,
                                 crls=[botan.X509CRL(**{"filename" if load_from_disk else "buf": crl}) for crl in crls] if crls else None)
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

    print(Botan.verify(args.CERTIFICATE, reference_time=args.reference_time, crls=args.crls))

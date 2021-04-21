#!/usr/bin/python3

import argparse
import datetime
import contextlib
import io
import os
import botan2 as botan


# noinspection PyBroadException
class Botan:
    TRUST_STORE_DIRECTORY = "/etc/pki/ca-trust/extracted/pem/"

    @staticmethod
    def verify(chain, reference_time=None, crls=None, **kwargs):
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
        is_setup_correctly = True

        if not os.path.isdir(Botan.TRUST_STORE_DIRECTORY):
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

    if Botan.is_setup_correctly():
        args = argument_parser.parse_args()
        print(Botan.verify(args.CERTIFICATE, reference_time=args.reference_time, crls=args.crls))
    else:
        print("Client is not set up correctly")

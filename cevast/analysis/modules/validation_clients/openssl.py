#!/usr/bin/python3

import argparse
import datetime
import os
import re
import subprocess


# noinspection PyBroadException
class Openssl:
    TRUST_STORE_FILE = "/etc/pki/tls/cert.pem"

    @staticmethod
    def verify(chain, reference_time=None, crls=None, **kwargs):
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
                                 required=False, default=int(datetime.datetime.today().date().strftime("%s")),
                                 dest="reference_time", metavar="DATE",
                                 help="reference date in format YYYY-MM-DD (Default is today)")
    argument_parser.add_argument("--crl", type=str, action="append", required=False, dest="crls", metavar="FILE",
                                 help="certificate revocation list (can be used multiple times)")
    argument_parser.add_argument("CERTIFICATE", type=str, nargs="+")

    if Openssl.is_setup_correctly():
        args = argument_parser.parse_args()
        print(Openssl.verify(args.CERTIFICATE, reference_time=args.reference_time, crls=args.crls))
    else:
        print("Client is not set up correctly")

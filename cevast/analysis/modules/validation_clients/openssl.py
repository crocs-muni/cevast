#!/usr/bin/python3

import argparse
import re
import subprocess


class Openssl:
    @staticmethod
    def verify(chain, reference_time=None, crls=None):
        chain = list(chain)

        try:
            command = ["openssl", "verify"]

            if reference_time:
                command += ["-attime", str(reference_time)]

            if crls:
                for crl in crls:
                    command += ["-CRLfile", crl]

                command += ["-crl_check"]

            command += [element for intermediate in reversed(chain[1:]) for element in ["-untrusted", intermediate]] if len(chain) > 1 else []
            command += [chain[0]]

            code = 0

            try:
                subprocess.check_output(command, stderr=subprocess.STDOUT)
            except subprocess.CalledProcessError as error:
                code = -1

                match = re.search(r'\nerror (\d+)', error.output.decode(encoding='utf-8'))

                if match:
                    code = match.group(1)
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

    print(Openssl.verify(args.CERTIFICATE, reference_time=args.reference_time, crls=args.crls))

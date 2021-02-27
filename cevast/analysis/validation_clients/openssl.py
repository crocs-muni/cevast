#!/usr/bin/python3

import re
import subprocess
import argparse


def verify(chain, referenceTime=None, crls=None):
    chain = list(chain)

    try:
        command = ["openssl", "verify"]

        if referenceTime:
            command += ["-attime", str(referenceTime)]

        if crls:
            for crl in crls:
                command += ["-CRLfile", crl]

            command += ["-crl_check"]

        command += [element for intermediate in reversed(chain[1:]) for element in ["-untrusted", intermediate]] if len(chain) > 1 else []
        command += [chain[0]]

        code = 0

        try:
            subprocess.check_output(command, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            code = -1

            match = re.search(r'\nerror (\d+)', e.output.decode(encoding='utf-8'))

            if match:
                code = match.group(1)
    except Exception:
        code = -1

    return str(code)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="chain format: ENDPOINT [INTERMEDIATE ...] [CA]")
    parser.add_argument("-t", type=int, required=False, dest="referenceTime", metavar="N", help="reference time (in seconds since the epoch)")
    parser.add_argument("--crl", type=str, action="append", required=False, dest="crls", metavar="FILE", help="certificate revocation list (can be used multiple times)")
    parser.add_argument("CERTIFICATE", type=str, nargs="+")

    args = parser.parse_args()

    print(verify(args.CERTIFICATE, args.referenceTime, args.crls))

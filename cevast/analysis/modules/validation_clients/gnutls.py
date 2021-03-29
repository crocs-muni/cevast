#!/usr/bin/python3

import argparse
import os
import subprocess


class GnuTLS:
    TRUST_STORE_FILE = "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem"

    @staticmethod
    def verify(chain, load_from_disk=True, reference_time=None, crl=None):
        chain = list(chain)

        try:
            command = []

            if reference_time:
                command += ["LD_PRELOAD={0}".format(os.path.join(os.path.dirname(os.path.realpath(__file__)), "libfaketime.so.1")),
                            "FAKETIME=$(date -d \"@{0}\" \"+%Y-%m-%d %H:%M:%S\")".format(str(reference_time))]

            command += ["certtool", "--load-ca-certificate", GnuTLS.TRUST_STORE_FILE]

            if crl:
                command += ["--load-crl", crl]

            command += ["--verify-profile", "low", "--verify"]

            command = [" ".join(command)]

            if load_from_disk:
                for i, certificate_path in enumerate(chain):
                    with open(certificate_path) as input_file:
                        chain[i] = input_file.read()

            process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)

            process.stdin.write("".join(chain).encode())
            process.stdin.close()

            output = process.stdout.read().decode()

            verified_chain_substring = "Chain verification output: Verified. The certificate is trusted."
            unverified_chain_substring = "Chain verification output: Not verified. The certificate is NOT trusted."

            if output.find(verified_chain_substring) != -1:
                result = "Verified"
            else:
                message_index = output.find(unverified_chain_substring)

                if message_index != -1:
                    result = "".join([word.capitalize() for word in output[message_index + len(unverified_chain_substring):].strip().split(".")[0].split()])

                    if result == "":
                        result = "Unknown"
                else:
                    result = "Error"
        except Exception:
            result = "Error"

        return result


if __name__ == "__main__":
    argument_parser = argparse.ArgumentParser(description="chain format: ENDPOINT [INTERMEDIATE ...] [CA]")

    argument_parser.add_argument("-t", type=int, required=False, dest="reference_time", metavar="N",
                                 help="reference time (in seconds since the epoch)")
    argument_parser.add_argument("--crl", type=str, required=False, dest="crl", metavar="FILE",
                                 help="certificate revocation list")
    argument_parser.add_argument("CERTIFICATE", type=str, nargs="+")

    args = argument_parser.parse_args()

    print(GnuTLS.verify(args.CERTIFICATE, reference_time=args.reference_time, crl=args.crl))

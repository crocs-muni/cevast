#!/usr/bin/python3

"""
This module contains an implementation of a certificate chain validation client for command-line GnuTLS.

The validation client can be both imported and used externally (as a standalone module) through the provided
command-line interface.

..Important::
  The validation client must be set up correctly before use. It is necessary to ensure, that GnuTLS is installed
  correctly, "libfaketime.so.1" binary (used for setting reference time) is present, and that `TRUST_STORE_FILE` is
  set to the path to the local trust store.

  When using the validation client externally, these prerequisites are always checked before validation is performed.
  When the validation client is imported, it is necessary to do these checks manually (i.e., using the method `is_setup_correctly()`).
"""

import argparse
import datetime
import os
import subprocess


# noinspection PyBroadException
class GnuTLS:
    """
    A class for certificate chain validation client utilizing command-line GnuTLS.

    Special error messages:

    "Error": any error / exception not related to the certificate chain validation (algorithm) itself

    "NotVerified": validation is unsuccessful for an unknown reason
    """

    TRUST_STORE_FILE = "/etc/pki/tls/cert.pem"

    @staticmethod
    def verify(chain, reference_time=None, crl=None, **kwargs):
        """
        Validates a certificate chain.

        `chain` is a list of paths to certificates forming a chain.
        `reference_time` is a reference time of validation in seconds since the epoch.
        `crl` is a path to CRL.
        `kwargs` are other, unexpected arguments.

        The returned result is a list of a set of error messages returned by command-line GnuTLS.
        """

        chain = list(chain)

        try:
            command = []

            if reference_time:
                command += GnuTLS.__get_faketime_command(reference_time)

            command += ["certtool", "--load-ca-certificate", GnuTLS.TRUST_STORE_FILE]

            if crl:
                command += ["--load-crl", crl]

            command += ["--verify-profile", "low", "--verify"]

            command = [" ".join(command)]

            for i, certificate_path in enumerate(chain):
                with open(certificate_path) as input_file:
                    chain[i] = input_file.read()

            process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)

            process.stdin.write("".join(chain).encode())
            process.stdin.close()

            command_output = process.stdout.read().decode(errors="ignore")

            verified_chain_substring = "Chain verification output: Verified. The certificate is trusted."
            unverified_chain_substring = "Chain verification output: Not verified. The certificate is NOT trusted."

            if command_output.find(verified_chain_substring) != -1:
                result = ["Verified"]
            else:
                message_index = command_output.find(unverified_chain_substring)

                if message_index != -1:
                    messages = []

                    for message in command_output[message_index + len(unverified_chain_substring):].strip().split("."):
                        if message != "":
                            messages.append("".join([word.capitalize() for word in message.split()]))

                    if len(messages) > 0:
                        result = sorted(set(messages))
                    else:
                        result = ["NotVerified"]

                else:
                    result = ["Error"]
        except Exception:
            result = ["Error"]

        return result

    @staticmethod
    def is_setup_correctly():
        """
        Verifies that the validation client is set up correctly. (GnuTLS is installed, reference time works,
        and trust store exists)
        """

        is_setup_correctly = True

        try:
            subprocess.check_output(" ".join(["certtool", "-v"]), stderr=subprocess.DEVNULL, shell=True)
        except Exception:
            is_setup_correctly = False

        if is_setup_correctly:
            try:
                command_output = subprocess.check_output(" ".join(GnuTLS.__get_faketime_command(0) + ["date", "-u"]),
                                                         stderr=subprocess.DEVNULL, shell=True)

                if command_output.decode().strip() != "Thu  1 Jan 01:00:00 CET 1970":
                    is_setup_correctly = False
            except Exception:
                is_setup_correctly = False

        if is_setup_correctly:
            if not os.path.isfile(GnuTLS.TRUST_STORE_FILE):
                is_setup_correctly = False

        return is_setup_correctly

    @staticmethod
    def __get_faketime_command(reference_time):
        return ["LD_PRELOAD={0}".format(os.path.join(os.path.dirname(os.path.realpath(__file__)), "libfaketime.so.1")),
                "FAKETIME=\"{0}\"".format(
                    datetime.datetime.fromtimestamp(reference_time).strftime("%Y-%m-%d %H:%M:%S"))]


if __name__ == "__main__":
    argument_parser = argparse.ArgumentParser(description="chain format: ENDPOINT [INTERMEDIATE ...] [CA]")

    argument_parser.add_argument("-r", type=lambda s: int(datetime.datetime.strptime(s, "%Y-%m-%d").timestamp()),
                                 required=False, dest="reference_date_as_time", metavar="DATE",
                                 help="reference date in format YYYY-MM-DD (at 00:00:00)")
    argument_parser.add_argument("-t", type=int, required=False, dest="reference_time", metavar="N",
                                 help="reference time in seconds since the epoch (surpassing reference date)")
    argument_parser.add_argument("--crl", type=str, required=False, dest="crl", metavar="FILE",
                                 help="certificate revocation list")
    argument_parser.add_argument("CERTIFICATE", type=str, nargs="+")

    if GnuTLS.is_setup_correctly():
        args = argument_parser.parse_args()
        print(GnuTLS.verify(args.CERTIFICATE, reference_time=args.reference_time if args.reference_time is not None else args.reference_date_as_time, crl=args.crl))
    else:
        print("Client is not set up correctly")

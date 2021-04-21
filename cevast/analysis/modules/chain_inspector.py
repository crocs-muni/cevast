#!/usr/bin/python3

import argparse
import itertools
from OpenSSL import crypto


# noinspection PyBroadException
class ChainInspector:
    @staticmethod
    def inspect(input_chain, load_from_disk=True, **kwargs):
        input_chain = list(input_chain)

        try:
            if load_from_disk:
                for i, certificate_path in enumerate(input_chain):
                    with open(certificate_path) as input_file:
                        input_chain[i] = input_file.read().encode()

            original_certificate_chain = [crypto.load_certificate(crypto.FILETYPE_PEM, certificate_content) for certificate_content in input_chain]
            original_pairs = ChainInspector.__get_subject_issuer_pairs(original_certificate_chain)

            if len(original_pairs) <= 8:
                result = "DISCONNECTED"

                if ChainInspector.__is_chain_continuous(original_pairs):
                    result = "OK"
                elif ChainInspector.__is_chain_continuous(list(reversed(original_pairs))):
                    result = "REVERSED"
                else:
                    for pair_permutation in list(itertools.permutations(original_pairs)):
                        if ChainInspector.__is_chain_continuous(list(pair_permutation)):
                            result = "REORDERED"
                            break
            else:
                result = "TOOLONG"

            result += "-{0}CA".format(ChainInspector.__get_ca_certificate_count(original_pairs))
        except Exception:
            result = "ERROR"

        return [result]

    @staticmethod
    def __get_subject_issuer_pairs(certificate_chain):
        pairs = []

        for certificate in certificate_chain:
            subject = str(certificate.get_subject())
            issuer = str(certificate.get_issuer())

            pairs.append([subject, issuer])

        return pairs

    @staticmethod
    def __is_chain_continuous(pairs):
        is_valid = True

        last_issuer = None

        for pair in pairs:
            subject = pair[0]
            issuer = pair[1]

            if last_issuer is not None:
                if subject != last_issuer:
                    is_valid = False
                    break

            last_issuer = issuer

        return is_valid

    @staticmethod
    def __get_ca_certificate_count(pairs):
        count = 0

        for pair in pairs:
            subject = pair[0]
            issuer = pair[1]

            if subject == issuer:
                count += 1

        return count


if __name__ == "__main__":
    argument_parser = argparse.ArgumentParser(description="chain format: CERTIFICATE [CERTIFICATE ...]")

    argument_parser.add_argument("CERTIFICATE", type=str, nargs="+")

    args = argument_parser.parse_args()

    print(ChainInspector.inspect(args.CERTIFICATE))

#!/usr/bin/python3

import argparse
import subprocess

def verify(chain, loadFromDisk=False, referenceTime=None, crls=None):
    command = ["certtool --verify-chain"]

    if loadFromDisk:
        for i, certificatePath in enumerate(chain):
            with open(certificatePath) as inputFile:
                chain[i] = inputFile.read()

    process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)

    process.stdin.write("".join(chain).encode())
    process.stdin.close()

    output = process.stdout.read().decode()

    verifiedChainSubstring = "Chain verification output: Verified. The certificate is trusted."
    unverifiedChainSubstring = "Chain verification output: Not verified. The certificate is NOT trusted."

    if output.find(verifiedChainSubstring) != -1:
        result = "Verified."
    else:
        messageIndex = output.find(unverifiedChainSubstring)

        if messageIndex != -1:
            result = output[messageIndex + len(unverifiedChainSubstring):].strip()
        else:
            result = "Unknown."

    return result

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="chain format: ENDPOINT [INTERMEDIATE ...] [CA]")
    parser.add_argument("-t", type=int, required=False, dest="referenceTime", metavar="N", help="reference time (in seconds since the epoch)")
    parser.add_argument("--crl", type=str, action="append", required=False, dest="crls", metavar="FILE", help="certificate revocation list (can be used multiple times)")
    parser.add_argument("CERTIFICATE", type=str, nargs="+")

    args = parser.parse_args()

    print(verify(args.CERTIFICATE, loadFromDisk=True, referenceTime=args.referenceTime, crls=args.crls))

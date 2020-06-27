"""This module contains various certificate validation functions."""

import sys
import re
import subprocess
import logging
from OpenSSL import crypto


log = logging.getLogger(__name__)


OK = "0"
UNKNOWN = "XX"


def cl_open_ssl(chain: list) -> str:
    """Validate SSL Certificate chain via command line openssl. Return result code."""
    try:
        inter = []
        server = chain[0]
        if len(chain) == 2:
            inter = ('--CAfile', chain[1])
        elif len(chain) > 2:
            inter = [v for elt in chain[1:] for v in ('-untrusted', elt)]
        # TODO check if only untrusted really works
        # openssl verify -show_chain -untrusted int.pem -untrusted int2.pem server.pem
        # openssl verify -show_chain --CAfile ca.pem server.pem
        subprocess.check_output(["openssl", "verify", "-show_chain", *inter, server], stderr=subprocess.STDOUT)
        return OK
    except subprocess.CalledProcessError as err:
        # print(e.cmd)
        match_object = re.search(r'\nerror (\d+)', err.output.decode(encoding='utf-8'))
        if match_object:
            return match_object.group(1)
        log.warning("FULL_ERROR: %s", err.output.decode(encoding='utf-8'))
        return UNKNOWN


def python_OpenSSL(chain: list) -> str:
    """Validate SSL Certificate chain using python OpenSSL library. Return result code."""
    inter = []
    server = chain[0]
    if len(chain) > 1:
        inter = chain[1:]
    try:
        # Load the server certificate
        with open(server) as cert_file:
            certificate = crypto.load_certificate(crypto.FILETYPE_PEM, cert_file.read())
        # Create a certificate store and add trusted certs
        store = crypto.X509Store()
        for cert in inter:
            with open(cert) as cert_file:
                int_certificate = crypto.load_certificate(crypto.FILETYPE_PEM, cert_file.read())
                store.add_cert(int_certificate)
        # Create a certificate context using the store and the server certificate
        store_ctx = crypto.X509StoreContext(store, certificate)
        # Verify the certificate, returns None if it can validate the certificate
        store_ctx.verify_certificate()

        return OK
    except crypto.X509StoreContextError as err:
        return str(err.args[0][0])
    except crypto.Error as err:
        log.warning("FULL_ERROR: %s", err)
        return UNKNOWN


if __name__ == "__main__":
    print('cl_open_ssl   : ', cl_open_ssl(sys.argv[1:]))
    print('python_OpenSSL: ', python_OpenSSL(sys.argv[1:]))
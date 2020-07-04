"""
This module provides various certificate validation methods.
Each method accepts a single argument -- list with cetificates file paths.
List should start with server certificate, followed by intermediates certificates
and end with trusted CA certificate.

To add an additional validation method, register the method under its name to global
variable METHODS in 'MODULE INITIALIZATION' section.

Module can be imported and used as a library. Import-safe functions should be used
to get validation method by name or get all the available methods:
  - show()     - return tuple with all available method names
  - get_all()  - return tuple with all available methods
  - get(name)  - return methody with given name

Module can also be run as a standalone script with following usage:
    python3 ./methods           - prints all available method names
    python3 ./methods c1 c2 cN  - prints validation results from all available methods,
                                  args c1-cN are provided to validation method as a chain that
                                  starts with server certificate and ends with CA
"""

import sys
import os
import re
import logging
import subprocess
from collections import OrderedDict


log = logging.getLogger(__name__)

# global dictionary hodling all available validation methods in this module under usage name
METHODS = OrderedDict()

OK = "0"
UNKNOWN = "XX"


def is_tool_available(name):
    """Check if the tool is installed and available on the system."""
    try:
        subprocess.Popen([name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except OSError as err:
        if err.errno == os.errno.ENOENT:
            return False
    return True


def get_all():
    """Get all available validation methods."""
    return tuple(METHODS.values())


def get(name: str):
    """Get validation method by name."""
    return METHODS.get(name, None)


def show():
    """Show available validation methods."""
    return tuple(METHODS.keys())


def _cl_open_ssl(chain: list) -> str:
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


def _PyOpenSSL(chain: list) -> str:
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


# -----------   MODULE INITIALIZATION   -------------------------
# try to load command-line openssl
log.info("Loading cli openssl")
if is_tool_available("openssl"):
    METHODS['openssl'] = _cl_open_ssl

# try to load PyOpenSSL
log.info("Loading PyOpenSSL")
try:
    from OpenSSL import crypto
    METHODS['pyopenssl'] = _PyOpenSSL
except ModuleNotFoundError:
    log.exception("PyOpenSSL failed to import - check if installed")


if __name__ == "__main__":
    if len(sys.argv) <= 1:
        print(show())
    else:
        for func in get_all():
            print(func(sys.argv[1:]))

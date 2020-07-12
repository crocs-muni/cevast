"""
This module provides various certificate validation methods.

.. important::
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

.. todo::
   Maybe would be better to analyse the certs before validation to identify CAs
   and intermediates (validation would be better then)
"""

import sys
import os
import re
import logging
import subprocess
from collections import OrderedDict

__author__ = 'Radim Podola'


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


def show(include_docstring: bool=False):
    """Show available validation methods."""
    if include_docstring:
        return tuple("{:<8} - {}".format(name, func.__doc__) for name, func in METHODS.items())
    return tuple(METHODS.keys())


def _openssl(chain: list) -> str:
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
        log.info("Validation failed")
        return UNKNOWN


def _pyopenssl(chain: list) -> str:
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
    except crypto.Error:
        log.info("Validation failed")
        return UNKNOWN


def _botan(chain: list) -> str:
    """Validate SSL Certificate chain using Botan library. Return result code."""
    inter = []
    cacert = None
    server = chain[0]
    if len(chain) == 2:
        cacert = chain[-1]
    elif len(chain) > 2:
        cacert = chain[-1]
        inter = chain[1:-1]
    try:
        # Load the server certificate
        server = botan.X509Cert(server)
        # Load untrusted subauthorities or set to None
        inter = [botan.X509Cert(i) for i in inter] if inter else None
        # Load CA cert or set to None
        cacert = [botan.X509Cert(cacert)] if cacert else None
        # Verify the certificate
        # Returns 0 if validation was successful, returns a positive error code if the validation was unsuccesful
        code = server.verify(intermediates=inter, trusted=cacert)

        return str(code)
    except botan.BotanException:
        log.exception("Validation failed: %s", ", ".join(chain))
        return UNKNOWN


# -----------   MODULE INITIALIZATION   -------------------------
# try to load command-line openssl
log.info("Loading cli openssl")
if is_tool_available("openssl"):
    METHODS['openssl'] = _openssl

# try to load PyOpenSSL
log.info("Loading PyOpenSSL")
try:
    from OpenSSL import crypto  # pylint: disable=E0401

    METHODS['pyopenssl'] = _pyopenssl
except ModuleNotFoundError:
    log.info("PyOpenSSL failed to import - check if installed")

# try to load Botan
log.info("Loading Botan")
try:
    import botan2 as botan  # pylint: disable=E0401

    METHODS['botan'] = _botan
except ModuleNotFoundError:
    log.info("Botan failed to import - check if installed")
except Exception:
    log.info("Botan failed to initialized - check if library is installed properly")


if __name__ == "__main__":
    if len(sys.argv) <= 1:
        print(show())
    else:
        for func in get_all():
            print(func(sys.argv[1:]))

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
# TODO rename to validation_methods
# TODO add support for reference time

import sys
import os
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


def show(include_docstring: bool = False):
    """Show available validation methods."""
    if include_docstring:
        return tuple("{:<8} - {}".format(name, func.__doc__) for name, func in METHODS.items())
    return tuple(METHODS.keys())


# -----------   MODULE INITIALIZATION   -------------------------

# try to load the chain inspector
# log.info("Loading chain inspection module")
# try:
#     from .modules.chain_inspector import ChainInspector
#
#     METHODS['chainInspector'] = ChainInspector.inspect
# except ModuleNotFoundError as error:
#     log.info("The client could not be loaded: {0}".format(error))

# try to load command-line Openssl
log.info("Loading CLI Openssl validation client")
try:
    from .modules.validation_clients.openssl import Openssl

    METHODS['openssl'] = Openssl.verify
except ModuleNotFoundError as error:
    log.info("The client could not be loaded: {0}".format(error))

# try to load PyOpenSSL
log.info("Loading PyOpenSSL validation client")
try:
    from .modules.validation_clients.pyopenssl import Pyopenssl

    METHODS['pyopenssl'] = Pyopenssl.verify
except ModuleNotFoundError as error:
    log.info("The client could not be loaded: {0}".format(error))

# try to load Botan
log.info("Loading Botan validation client")
try:
    from .modules.validation_clients.botan import Botan

    METHODS['botan'] = Botan.verify
except ModuleNotFoundError as error:
    log.info("The client could not be loaded: {0}".format(error))

# try to load command-line GnuTLS
log.info("Loading CLI GnuTLS validation client")
try:
    from .modules.validation_clients.gnutls import GnuTLS

    METHODS['gnutls'] = GnuTLS.verify
except ModuleNotFoundError as error:
    log.info("The client could not be loaded: {0}".format(error))


if __name__ == "__main__":
    if len(sys.argv) <= 1:
        print(show())
    else:
        for func in get_all():
            print(func(sys.argv[1:]))

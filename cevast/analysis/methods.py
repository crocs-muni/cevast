"""
This module provides access to certificate chain validation clients and other analytical modules.

.. important::
   Each validation client / module is bound through a method that requires a single argument --- a list of paths to
   certificates forming a chain (starting with server's certificate).

To add an additional validation client / module, register it under its name to global
variable METHODS in 'MODULE INITIALIZATION' section.

The module can be imported and used as a library. Import-safe functions should be used
to get validation clients / modules by name or get all that are available:

- show()     - return tuple with all available method names

- get_all()  - return tuple with all available methods

- get(name)  - return method with given name
"""

import sys
import os
import logging
import subprocess
from collections import OrderedDict

__author__ = 'Radim Podola'


log = logging.getLogger(__name__)

# global dictionary holding all available validation methods in this module under usage name
METHODS = OrderedDict()


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

log.info("Loading chain inspection module")
try:
    from .modules.chain_inspector import ChainInspector

    METHODS["chainInspector"] = ChainInspector.inspect
except ModuleNotFoundError as error:
    log.info("The client could not be loaded: {0}".format(error))

log.info("Loading CLI Openssl validation client")
try:
    from .modules.validation_clients.openssl import Openssl

    if Openssl.is_setup_correctly():
        METHODS["openssl"] = Openssl.verify
    else:
        log.info("The client is not set up correctly")
except ModuleNotFoundError as error:
    log.info("The client could not be loaded: {0}".format(error))

log.info("Loading PyOpenSSL validation client")
try:
    from .modules.validation_clients.pyopenssl import Pyopenssl

    if Pyopenssl.is_setup_correctly():
        METHODS["pyopenssl"] = Pyopenssl.verify
    else:
        log.info("The client is not set up correctly")
except ModuleNotFoundError as error:
    log.info("The client could not be loaded: {0}".format(error))

log.info("Loading Botan validation client")
try:
    from .modules.validation_clients.botan import Botan

    if Botan.is_setup_correctly():
        METHODS["botan"] = Botan.verify
    else:
        log.info("The client is not set up correctly")
except ModuleNotFoundError as error:
    log.info("The client could not be loaded: {0}".format(error))

log.info("Loading CLI GnuTLS validation client")
try:
    from .modules.validation_clients.gnutls import GnuTLS

    if GnuTLS.is_setup_correctly():
        METHODS["gnutls"] = GnuTLS.verify
    else:
        log.info("The client is not set up correctly")
except ModuleNotFoundError as error:
    log.info("The client could not be loaded: {0}".format(error))

log.info("Loading CLI MbedTLS validation client")
try:
    from .modules.validation_clients.mbedtls import MbedTLS

    if MbedTLS.is_setup_correctly():
        METHODS["mbedtls"] = MbedTLS.verify
    else:
        log.info("The client is not set up correctly")
except ModuleNotFoundError as error:
    log.info("The client could not be loaded: {0}".format(error))


if __name__ == "__main__":
    if len(sys.argv) <= 1:
        print(show())
    else:
        for func in get_all():
            print(func(sys.argv[1:]))

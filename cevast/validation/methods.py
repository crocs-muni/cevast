"""This module contains various certificate validation functions."""

import re
import subprocess
import logging


log = logging.getLogger(__name__)


def cl_open_ssl(chain: list) -> str:
    """Validate SSL Certificate chain via command line openssl. Return result code."""
    try:
        inter = []
        server = chain[0]
        if len(chain) > 1:
            inter = [v for elt in chain[1:] for v in ('-untrusted', elt)]
        # openssl verify -show_chain -untrusted int.pem server.pem
        subprocess.check_output(["openssl", "verify", "-show_chain", *inter, server], stderr=subprocess.STDOUT)
        return "0"
    except subprocess.CalledProcessError as err:
        # print(e.cmd)
        match_object = re.search(r'\nerror (\d+)', err.output.decode(encoding='utf-8'))
        if match_object:
            return match_object.group(1)
        log.warning("FULL_ERROR: %s", err.output.decode(encoding='utf-8'))
        return "XX"

"""
This module contains helper functions for project logging.
"""

import os
import sys
import gzip
import logging
import logging.handlers

__author__ = 'Radim Podola'

LOG_DIR = './log'
LOG_FILENAME = 'cevast.log'


def __namer(name):
    return name + ".gz"


def __rotator(source, dest):
    with open(source, "rb") as src:
        with gzip.open(dest, "wb") as trg:
            trg.writelines(src)
    os.remove(source)


def setup_cevast_logger(debug: bool = False) -> logging.Logger:
    """
    Setup the project logger 'CEVAST'.

    Logger has configured 2 handlers:
        - console_handler to write error-like logs to stdout (>= WARNING)
        - file_handler to write logs into rotating file with compression of rotated files

    Each module-level logger must start with 'cevast.' to inherit the setup.

    TODO: add support for config file
    """
    if not os.path.exists(LOG_DIR):
        os.mkdir(LOG_DIR)

    # Setup formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %H:%M:%S')

    # Setup handler writing error-like logs to console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.WARNING)
    console_handler.setFormatter(formatter)

    # Setup handler writing logs to a file with compressing of the rotated files
    file_handler = logging.handlers.RotatingFileHandler(os.path.join(LOG_DIR, LOG_FILENAME), maxBytes=10000000, backupCount=30)
    file_handler.setFormatter(formatter)
    file_handler.rotator = __rotator
    file_handler.namer = __namer

    # Setup project logger, not root
    logger = logging.getLogger('cevast')
    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

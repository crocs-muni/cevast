"""This module contains ChainValidator implementation of CertValidator interface."""

import os
import logging
import multiprocessing
import shutil
from typing import List
from cevast.certdb import CertDB, CertNotAvailableError
from cevast.utils import make_PEM_filename
from .validator import CertValidator
from .methods import cl_open_ssl


log = logging.getLogger(__name__)


# TODO fix logging into rotatefilehandler within multiprocessing
class ChainValidator(CertValidator):
    """
    CertValidator implementation that validates certificate chains. Validate function
    accepts host name and list of certificate IDs (fingerprints). Those certificates are
    searched in provided CertDB.

    Special key arguments:
    [mandatory] `certdb` is an instance of CertDB, where the certificates will be taken from,
    [optional] `export_dir` is a directory that will be used for temporary operations
        with certificates. Directory will be clean-up upon calling `done`.
    """

    def __init__(self, output_file: str, processes: int, **kwargs):
        # Init common arguments
        self.__single = processes == 0
        self.__out = open(output_file + '.csv', 'w')
        # Init special arguments
        self.__certdb: CertDB = kwargs.get('certdb', None)
        if self.__certdb is None:
            raise ValueError('Mandatory certdb argument must be provided withing kwargs.')
        self.__export_dir = kwargs.get('export_dir', None)
        if self.__export_dir is None:
            self.__export_dir = './tmp_chain_validator/'
            os.makedirs(self.__export_dir, exist_ok=True)
            self.__cleanup_export_dir = True
        else:
            self.__cleanup_export_dir = False
        # Initialize pool and workers
        if not self.__single:
            self.__pool = multiprocessing.Pool(processes)

        log.info("ChainValidator created: output_file=%s, processes=%d", output_file, processes)

    def schedule(self, host: str, chain: List[str]) -> None:
        # Get certificate files
        pems = []
        for cert in chain:
            # check if already exported first
            path = self.__export_dir + make_PEM_filename(cert)
            if not os.path.exists(path):
                try:
                    path = self.__certdb.export(cert, self.__export_dir, False)
                except CertNotAvailableError:
                    log.info("HOST <%s> has broken chain", host)
                    return ""
            pems.append(path)
        # Validate
        if self.__single:
            self.__out.write(ChainValidator.validate(host, pems))
        else:
            self.__pool.apply_async(ChainValidator.validate, args=(host, pems), callback=self.__out.write)

    def done(self) -> None:
        # Wait for workers to finish
        if not self.__single:
            self.__pool.close()
            self.__pool.join()
        # Close output file
        self.__out.flush()
        self.__out.close()
        # Clean up own export dir
        if self.__cleanup_export_dir:
            shutil.rmtree(self.__export_dir)

    @staticmethod
    def validate(host: str, chain: List[str]) -> str:
        """
        Validation function of single validation task. Return formatted result.
        `host` is host name,
        `chain` is list of paths to certificates files in PEM format forming
        SSL Certificate Chain (strating with server certificate).
        """
        result = []
        # Call cl_open_ssl validation
        result.append(cl_open_ssl(chain))
        # Call other validations here....

        return "{}, {}, {}\n".format(host.rjust(15), ",".join(result), " -> ".join(chain))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if not self.__single:
            self.__pool.terminate()
        self.__out.close()

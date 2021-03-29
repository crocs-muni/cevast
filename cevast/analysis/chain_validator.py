"""This module contains ChainValidator implementation of CertAnalyser interface."""

import os
import logging
import multiprocessing
import shutil
import signal
import datetime
from typing import List
from cevast.certdb import CertDB, CertNotAvailableError
from cevast.utils import make_PEM_filename
from .cert_analyser import CertAnalyser
from .methods import get_all, get, show

log = logging.getLogger(__name__)


# TODO fix logging into rotatefilehandler within multiprocessing
class ChainValidator(CertAnalyser):
    """
    CertAnalyser implementation that validates certificate chains. Validation function
    accepts host name and list of certificate IDs (fingerprints). Those certificates are
    searched in provided CertDB.

    Result is stored as CSV file in following format:
    {host, validation method 1, validation method 2, validation method N, chain}
    .. hint::
       Such format can be easily analyzed. E.g. to count number of each error code one could use:
       awk -F "\"*,\"*" '{print $2}' cevast_repo/RAPID/VALIDATED/20200616_12443.csv | sort | uniq -c

    Special key arguments:
    [mandatory] `certdb` is an instance of CertDB, where the certificates will be taken from.
    [optional] `export_dir` is a directory that will be used for temporary operations
        with certificates. Directory will be clean-up upon calling `done`.
    [optional] `methods` is a list with validation methods to use.
    """

    def __init__(self, output_file: str, processes: int, **kwargs):
        # Init common arguments
        self.__single = processes == 0
        self.__out = open(output_file + '.csv', 'w')

        # Init validation methods
        self.__methods = kwargs.get('methods', None)
        if self.__methods is None:
            methods = get_all()
        else:
            methods = [get(name) for name in self.__methods]
        if not methods:
            raise ValueError("No validation methods are available -> nothing to do")
        # write validation header
        self.__out.write("{}, {}, {}\n".format('HOST', ", ".join(show()), "CHAIN"))

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
        self.__reference_date: datetime.date = kwargs.get('reference_date', None)
        if self.__reference_date is None:
            raise ValueError('Mandatory reference_date argument must be provided withing kwargs.')
        log.info("Reference date: {0}, ({1})".format(self.__reference_date, int(self.__reference_date.strftime("%s"))))

        self.__lock = multiprocessing.Lock()

        # Initialize pool and workers
        if not self.__single:
            self.__pool = multiprocessing.Pool(processes,
                                               initializer=ChainValidator.__init_worker,
                                               initargs=(self.__certdb,
                                                         self.__export_dir,
                                                         methods,
                                                         self.__reference_date,
                                                         self.__lock,
                                                         True))
        else:
            ChainValidator.__init_worker(self.__certdb, self.__export_dir, methods, self.__reference_date, self.__lock)

        log.info("ChainValidator created: output_file=%s, processes=%d", output_file, processes)

    @staticmethod
    def __init_worker(certdb: CertDB, tmp_dir: str, methods: list, reference_date: datetime.date,
                      lock: multiprocessing.Lock, ignore_sigint: bool = False):
        """Create and initialize global variables used in validate method. {Not nice, but working well
        with multiprocessing pool -> sharing instance of CertDB - object is not copied because of copy-on-write fork()}
        """
        global WORKER_CERTDB
        global WORKER_TMP_DIR
        global VALIDATION_METHODS
        global REFERENCE_DATE
        global LOCK
        WORKER_CERTDB = certdb
        WORKER_TMP_DIR = tmp_dir
        VALIDATION_METHODS = methods
        REFERENCE_DATE = reference_date
        LOCK = lock
        if ignore_sigint:
            # let worker processes ignore SIGINT, parent will cleanup pool via teminate()
            signal.signal(signal.SIGINT, signal.SIG_IGN)

    def schedule(self, host: str, chain: List[str]) -> None:
        if self.__single:
            self.__out.write(ChainValidator._validate(host, chain))
        else:
            self.__pool.apply_async(ChainValidator._validate, args=(host, chain), callback=self.__out.write)

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
    def _validate(host: str, chain: List[str]) -> str:
        """
        Validation function of single validation task. Return formatted result.
        `host` is host name,
        `chain` is list of certificate IDs forming SSL Certificate Chain (starting with server certificate).
        """
        result = []
        pems = []

        # check if already exported first
        LOCK.acquire()
        try:
            for cert in chain:
                # TODO make some structure to not overload single directory
                path = WORKER_TMP_DIR + make_PEM_filename(cert)
                if not os.path.exists(path):
                    try:
                        path = WORKER_CERTDB.export(cert, WORKER_TMP_DIR, False)
                    except CertNotAvailableError:
                        log.info("HOST <%s> has broken chain", host)
                        return ""
                pems.append(path)
        finally:
            LOCK.release()

        validation_method_arguments = {"reference_time": int(REFERENCE_DATE.strftime("%s"))}

        # Call validation methods
        for method in VALIDATION_METHODS:
            result.append(method(pems, **validation_method_arguments))

        return "{}, {}, {}\n".format(host.rjust(15), ", ".join(result), ", ".join(chain))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if not self.__single:
            self.__pool.terminate()
        self.__out.close()

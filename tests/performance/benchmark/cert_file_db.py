"""
    This modul contains benchmark script testing performance of modul cevast.certdb.cert_file_db

Run as:> python3 -m cProfile -s calls benchmark_CertFileDB.py perf_st 10000-certs.gz > profiles/{commit}_cert_file_db
"""

import sys
import time
import shutil
import cevast.dataset.parsers as parser
from cevast.certdb import CertFileDB, CertFileDBReadOnly
from cevast.utils.cert_utils import BASE64_to_PEM

storage = sys.argv[1]
dataset = sys.argv[2]
certs = []

try:
    certdb = CertFileDB(storage)
except ValueError:
    CertFileDB.setup(storage, owner='cevast', desc='Cevast CertFileDB for performance tests')
    certdb = CertFileDB(storage)

certdb_rdonly = CertFileDBReadOnly(storage)

print("Benchmark: %s" % __file__)
print("Dataset: %s" % dataset)
print()
print("Started insert:")
t0 = time.time()
for sha, cert in parser.RapidParser.read_certs(dataset):
    certdb.insert(sha, BASE64_to_PEM(cert))
    certs.append(sha)
print("Finished: %r" % (time.time() - t0))
print()
print("Check every cert for existance:")
t0 = time.time()
assert certdb.exists_all(certs)
print("Finished: %r" % (time.time() - t0))
print()
print("Started rollback: ")
t0 = time.time()
certdb.rollback()
print("Finished: %r" % (time.time() - t0))
print()
print("Started 2nd insert:")
t0 = time.time()
for sha, cert in parser.RapidParser.read_certs(dataset):
    certdb.insert(sha, BASE64_to_PEM(cert))
print("Finished: %r" % (time.time() - t0))
print()
print("Started commit: ")
t0 = time.time()
certdb.commit()
print("Finished: %r" % (time.time() - t0))
print()
print("Check every cert for existance (ReadOnly):")
t0 = time.time()
assert certdb_rdonly.exists_all(certs)
print("Finished: %r" % (time.time() - t0))
print()
print("Started get:")
t0 = time.time()
for cert in certs:
    certdb.get(cert)
print("Finished: %r" % (time.time() - t0))
print()
print("Started delete: ")
t0 = time.time()
for cert in certs:
    certdb.delete(cert)
print("Finished: %r" % (time.time() - t0))
print()
print("Started commit: ")
t0 = time.time()
certdb.commit()
print("Finished: %r" % (time.time() - t0))

shutil.rmtree(storage, ignore_errors=True)
"""
    This modul contains benchmark script testing performance of modul cevast.certdb.cert_file_db

Run as:> python3 -m cProfile -s calls cert_file_db.py {storage} {10000_certs.gz} {CPUs} > profiles/{commit}_cert_file_db_CPUs
"""

import sys
import time
import shutil
import cevast.dataset.unifiers as unifier
from cevast.certdb import CertFileDB, CertFileDBReadOnly
from cevast.utils.cert_utils import BASE64_to_PEM
from cevast.utils.logging import setup_cevast_logger

setup_cevast_logger(debug=True, process_id=True)

storage = sys.argv[1]
dataset = sys.argv[2]
try:
    cpus = sys.argv[3]
except IndexError:
    cpus = 1
certs = []

try:
    certdb = CertFileDB(storage, cpus)
except ValueError:
    CertFileDB.setup(storage, owner='cevast', desc='Cevast CertFileDB for performance tests')
    certdb = CertFileDB(storage, cpus)

certdb_rdonly = CertFileDBReadOnly(storage)

print("Benchmark: %s" % __file__)
print("Dataset: %s" % dataset)
print("CPUs used: %s" % cpus)
print()
print("Started insert:")
t0 = time.time()
for sha, cert in unifier.RapidUnifier.parse_certs(dataset):
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
for sha, cert in unifier.RapidUnifier.parse_certs(dataset):
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
print("Check every cert for existance 2nd time (ReadOnly):")
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
print("Started delete every 2nd cert: ")
t0 = time.time()
for cert in certs[::2]:
    certdb.delete(cert)
print("Finished: %r" % (time.time() - t0))
print()
print("Started commit: ")
t0 = time.time()
certdb.commit()
print("Finished: %r" % (time.time() - t0))

shutil.rmtree(storage, ignore_errors=True)

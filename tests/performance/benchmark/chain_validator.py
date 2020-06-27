"""This modul contains benchmark script testing performance of ChainValidator

Run as:> python3 -m cProfile -s calls chain_validator.py {CertDB} {chain_file} {CPUs} > profiles/{commit}_{CPUs}_cpu_chain_validator.txt
"""

import os
import sys
import time
from cevast.dataset.parsers import RapidParser
from cevast.validation import ChainValidator
from cevast.certdb import CertFileDB
from cevast.utils.logging import setup_cevast_logger


log = setup_cevast_logger(debug=True, process_id=True)

# Setup benchmark
storage = sys.argv[1]
chain_file = sys.argv[2]
try:
    cpus = int(sys.argv[3])
except IndexError:
    cpus = os.cpu_count()

try:
    certdb = CertFileDB(storage, cpus)
except ValueError:
    CertFileDB.setup(storage, owner='cevast', desc='Cevast CertFileDB for performance tests')
    certdb = CertFileDB(storage, cpus)

filename = "{}_{}.csv".format(chain_file, cpus)

print("Benchmark: %s" % __file__)
print("Dataset: %s" % chain_file)
print("CPUs used: %s" % cpus)
print()
print("Started validation:")
t0 = time.time()
# Open validator as context manager
with ChainValidator(filename, cpus, **{'certdb': certdb}) as validator_ctx:
    for host, chain in RapidParser.read_chains(chain_file):
        validator_ctx.schedule(host, chain)
    # Indicate that no more validation data will be scheduled
    validator_ctx.done()
print("Finished: %r" % (time.time() - t0))

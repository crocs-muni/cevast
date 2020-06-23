import sys
import json
import cevast.dataset.parsers as parser
from cevast.certdb import CertFileDB
from cevast.dataset import DatasetManagerFactory
from cevast.utils.logging import setup_cevast_logger


#Will work with DatasetManager
# will pass config file to function as **kwargs

log = setup_cevast_logger(debug=False)
log.info('Starting')

storage = sys.argv[1]
dataset_id = sys.argv[2]
repo = sys.argv[3]

try:
    db = CertFileDB(storage, 64)
except ValueError:
    CertFileDB.setup(storage, owner='cevast', desc='Cevast CertFileDB')
    db = CertFileDB(storage, 64)

manager = DatasetManagerFactory.get_manager("RAPID")

manager(repo, ports='50880').run([], db)
exit()
rapid_parser = parser.RapidParser(dataset_id + "_certs.gz", dataset_id + "_hosts.gz", dataset_id + "_chain.gz", dataset_id + "_broken_chain.gz")

try:
    rapid_parser.store_certs(db)
except (OSError, ValueError):
    log.exception("Fatal error during parsing certificates -> rollback and return")
    db.rollback()
    exit(2)

try:
    rapid_parser.store_chains(db)
    # Commit inserted certificates now
    # certdb.exists_all is faster before commit
    db.commit()
    # Store dataset parsing log
    log_filename = dataset_id + '.json'
    log_str = json.dumps(rapid_parser.parsing_log, sort_keys=True, indent=4)
    log.info('Storing parsing log about dataset: %s', dataset_id)
    log.debug(log_str)
    with open(log_filename, 'w') as outfile:
        outfile.write(log_str)
except OSError:
    log.exception("Fatal error during parsing host scans -> commit and return")
    db.commit()
    exit(2)


# TODO change validator -> validation modul with function like validate_chain(cfg_validators)
#   - rather only validation logic with registered validators not related to dataset

# TODO make dataset_manager generic implementing some interface
#   - will magage DB, so even can make different implementation based on DB type
#   - will manage work pipline related to dataset_type, so specific dataset_manager will only use the utility functions from submodules as parsers
#      - some list argument work_pipeline would be nice - user can specify on commandline or cfg
#   - can then improve pipeline liek not commit DB after parsing and immediatelly analyse not zipped files, or even analyse during parsing of hosts
#        - parsers will have some generator function yielding host chains
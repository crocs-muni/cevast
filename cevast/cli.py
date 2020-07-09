import os
import argparse
from typing import List
from datetime import datetime
from cevast.certdb import CertFileDB
from cevast.dataset import DatasetManagerFactory, DatasetManagerTask, DatasetType
from cevast.utils.logging import setup_cevast_logger
from cevast.analysis import ChainValidator


# based on cpus set process_id
log = setup_cevast_logger(debug=False, process_id=True)


def valid_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('type', type=str.upper, choices=[str(t) for t in DatasetType])
    parser.add_argument('repository', nargs='?', default=os.getcwd())
    parser.add_argument("-d",
                        "--date",
                        help="The Date ID - format YYYY-MM-DD",
                        required=True,
                        type=valid_date)
    parser.add_argument('--certdb', required=True)
    parser.add_argument('-t', '--task', action='append', type=str.upper, choices=[str(t) for t in DatasetManagerTask])
    parser.add_argument('--port', default='443')
    parser.add_argument('--cpu', default=os.cpu_count() - 1, type=int)

    return parser.parse_args()


# If only validation -> use CertDBReadonly!!!


def cli():
    """Runs the cli."""
    log.info('Starting')
    args = parse_args()
    log.info(args)

    try:
        db = CertFileDB(args.certdb, args.cpu)
    except ValueError:
        log.info('CertFileDB does not exist yet, will be created.')
        CertFileDB.setup(args.certdb, owner='cevast', desc='Cevast CertFileDB')
        db = CertFileDB(args.certdb, args.cpu)

    manager = DatasetManagerFactory.get_manager(args.type)(args.repository, date=args.date, ports=args.port, cpu_cores=args.cpu)

    if not args.task:
        log.warning('Nothing to do, yeaaaa')
        return None

    tasks = []
    #analyser_cfg = {'certdb': db, 'methods': ['botan']}
    analyser_cfg = {'certdb': db}
    for args_task in args.task:
        if DatasetManagerTask.validate(args_task):
            task = DatasetManagerTask[args_task]
            params = {}
            if task == DatasetManagerTask.COLLECT:
                pass
            elif task == DatasetManagerTask.UNIFY:
                params['certdb'] = db
            elif task == DatasetManagerTask.ANALYSE:
                params['analyser_cfg'] = analyser_cfg
                params['analyser'] = ChainValidator

            tasks.append((task, params))
    print(tasks)
    manager.run(tasks)

    db.commit()


if __name__ == "__main__":
    cli()
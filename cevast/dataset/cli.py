"""Group of CLI commands used for Dataset management and tasks."""

from datetime import datetime
import click
from .dataset import Dataset, DatasetRepository, DatasetType, DatasetState

__author__ = 'Radim Podola'


def _validate_cli_date(_, __, value):
    """Validates format of date."""
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        raise click.BadParameter('Date need to be in format YYYY-mm-dd')


def _validate_cli_filter_date(_, __, value):
    """Validates format of date filter."""
    try:
        if not value or len(value) > 8:
            raise click.BadParameter('Date filter need to be in format YYYYmmdd (can be partial e.g. only YYYYmm)')
        int(value)  # test for number
        return value
    except ValueError:
        raise click.BadParameter('Date filter need to be in format YYYYmmdd (can be partial e.g. only YYYYmm)')


# -------------------------- DatasetRepository CLI --------------------------
@click.group('repository')
@click.argument('directory', type=click.Path(exists=True))
@click.pass_context
def dataset_repository_group(ctx, directory):
    """Gives access to Dataset Repository at <DIRECTORY>."""
    ctx.obj['repo'] = DatasetRepository(directory)


@dataset_repository_group.command('show')
@click.option('--type', '-t', 'type_', type=click.Choice([str(t) for t in DatasetType]), help='Dataset Type to filter.')
@click.option('--state', '-s', type=click.Choice([str(t) for t in DatasetState]), help='Dataset State to filter.')
@click.option(
    '--date',
    '-d',
    callback=_validate_cli_filter_date,
    help='Dataset date to filter in format [YYYYMMDD] (only part of date can be set).',
)
@click.pass_context
def dataset_repository_show(ctx, type_, state, date):
    """Show datasets in <DIRECTORY> matching given filter(s)."""
    click.echo(ctx.obj['repo'].dumps(dataset_id=date or '', dataset_type=type_, state=state))


# ------------------------------- Dataset CLI -------------------------------
@click.group('dataset')
@click.argument('directory', type=click.Path(exists=True))
@click.option(
    '--type', '-t', 'type_', required=True, type=click.Choice([str(t) for t in DatasetType]), help='Dataset Type to filter.'
)
@click.option(
    '--state', '-s', required=True, type=click.Choice([str(t) for t in DatasetState]), help='Dataset State to filter.'
)
@click.option(
    '--date',
    '-d',
    required=True,
    callback=_validate_cli_date,
    help='Dataset date to filter in format [YYYYMMDD] (only part of date can be set).',
)
@click.version_option()
@click.pass_context
def dataset_group(ctx, storage, cpu, read_only):
    """Gives access to CertDB at <DIRECTORY>."""
    ctx.ensure_object(dict)


"""
    CertFileDB.setup(storage, owner='cevast', desc='Cevast CertFileDB')

    try:
        db = CertFileDB(args.certdb, args.cpu)
    except ValueError:
        log.info('CertFileDB does not exist yet, will be created.')
        CertFileDB.setup(args.certdb, owner='cevast', desc='Cevast CertFileDB')
        db = CertFileDB(args.certdb, args.cpu)

    manager = DatasetManagerFactory.get_manager(args.type)(args.repository, date=args.date, ports=args.port, cpu_cores=args.cpu)

    ctx.obj['manager'] = manager
"""


@dataset_group.command('collect')
@click.pass_context
def dataset_collect(ctx):
    """Collects dataset(s) matching given filter(s)."""


@dataset_group.command('unify')
@click.pass_context
def dataset_unify(ctx):
    """Unifies dataset(s) matching given filter(s)."""


# If only validation -> use CertDBReadonly!!!
@dataset_group.command('analyse')
@click.pass_context
def dataset_analyse(ctx):
    """Analyses dataset(s) matching given filter(s)."""


@dataset_group.command('run')
@click.pass_context
def dataset_run(ctx):
    """Runs Task pipeline for dataset(s) matching given filter(s)."""


"""
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
"""


if __name__ == "__main__":
    dataset_group()  # pylint: disable=E1120

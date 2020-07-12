"""Group of CLI commands used for Dataset management and tasks.

   Entry point of `datasetmanager` and `datasetrepository` CLI.
"""

import os
from datetime import datetime
import click
from cevast.certdb import CertFileDB, CertFileDBReadOnly
from cevast.utils.logging import setup_cevast_logger
from cevast.analysis import ChainValidator
from .dataset import DatasetRepository, DatasetType, DatasetState
from .manager_factory import DatasetManagerFactory, DatasetInvalidError
from .managers import DatasetManagerTask

__author__ = 'Radim Podola'

CLI_DATE_FORMAT = '%Y-%m-%d'


def _validate_cli_date(_, __, value):
    """Validates format of date."""
    try:
        return datetime.strptime(value, CLI_DATE_FORMAT).date()
    except ValueError:
        raise click.BadParameter('Date need to be in format YYYY-mm-dd')


def _validate_cli_filter_date(_, __, value):
    """Validates format of date filter."""
    try:
        if value is None:
            return None
        if not value or len(value) > 8:
            raise click.BadParameter('Date filter need to be in format YYYYmmdd (can be partial e.g. only YYYYmm)')
        int(value)  # test for number
        return value
    except ValueError:
        raise click.BadParameter('Date filter need to be in format YYYYmmdd (can be partial e.g. only YYYYmm)')


def _prepare_analyser_arg(certdb):
    return {'analyser_cfg': {'certdb': certdb},
            'analyser': ChainValidator}


# -------------------------- DatasetRepository CLI --------------------------
@click.group('repository')
@click.argument('directory', type=click.Path(exists=True))
@click.pass_context
def dataset_repository_group(ctx, directory):
    """Gives access to Dataset Repository at <DIRECTORY>."""
    ctx.ensure_object(dict)
    if ctx.parent is None:  # Check if was called diretly via "datasetrepository" alias and should set up logger then
        setup_cevast_logger()

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
@click.group('manager')
@click.argument('directory', type=click.Path(exists=True))
@click.option(
    '--type', '-t', 'type_', required=True, type=click.Choice([str(t) for t in DatasetType]), help='Dataset Type.'
)
@click.option(
    '--date',
    '-d',
    default=datetime.today().date().strftime(CLI_DATE_FORMAT),
    callback=_validate_cli_date,
    help='Dataset date in format [YYYY-mm-dd].',
)
@click.option(
    '--port', '-p', default=[443], multiple=True, help='Dataset Port(s).'
)
@click.option('--cpu', type=int, help='Max Number of CPU cores to use.')
@click.version_option()
@click.pass_context
def manager_group(ctx, directory, type_, date, port, cpu):
    """Gives access to specified Dataset management."""
    ctx.ensure_object(dict)
    if cpu is None:
        cpu = ctx.obj.get('cpu', os.cpu_count() - 1)  # Might be specified on top level command
    ctx.obj['cpu'] = cpu  # Pass it to subcommands

    if ctx.parent is None:  # Check if was called diretly via "manager" alias and should set up logger then
        setup_cevast_logger(process_id=cpu > 1)

    try:
        manager = DatasetManagerFactory.get_manager(type_)(repository=directory, date=date, ports=port, cpu_cores=cpu)
    except DatasetInvalidError as err:
        click.echo('Failed to load manager: {}'.format(err))
        ctx.exit(1)

    ctx.obj['manager'] = manager


@manager_group.command('collect')
@click.option('--api_key', help='API Key might needed for collection.')
@click.pass_context
def manager_collect(ctx, api_key):
    """Collects dataset(s) matching given filter(s)."""
    collected = ctx.obj['manager'].collect(api_key)
    click.echo('Collected Datasets: {}'.format(collected))


@manager_group.command('unify')
@click.option(
    '--certdb',
    required=True,
    type=click.Path(exists=True),
    help='Path to CertDB where the certificates should be stored into.'
)
@click.pass_context
def manager_unify(ctx, certdb):
    """Unifies dataset(s) matching given filter(s)."""
    try:
        certdb = CertFileDB(certdb, ctx.obj['cpu'])
    except ValueError:
        click.echo(
            'CertFileDB does not exist at {} yet, run "cevast certdb setup --help" for more information'.format(certdb)
        )
        ctx.exit(1)

    unified = ctx.obj['manager'].unify(certdb)
    certdb.commit()
    click.echo('Unified Datasets: {}'.format(unified))


@manager_group.command('analyse')
@click.option(
    '--certdb',
    required=True,
    type=click.Path(exists=True),
    help='Path to CertDB where the certificates should be read from.'
)
@click.pass_context
def manager_analyse(ctx, certdb):
    """Analyses dataset(s) matching given filter(s)."""
    try:
        certdb = CertFileDBReadOnly(certdb)
    except ValueError:
        click.echo(
            'CertFileDB does not exist at {} yet, run "cevast certdb setup --help" for more information'.format(certdb)
        )
        ctx.exit(1)

    analysed = ctx.obj['manager'].analyse(**_prepare_analyser_arg(certdb))
    click.echo('Analysed Datasets: {}'.format(analysed))


@manager_group.command('runner')
@click.option(
    '--certdb',
    required=True,
    type=click.Path(exists=True),
    help='Path to CertDB where the certificates should be stored/read to/from.'
)
@click.option(
    '--task',
    '-t',
    type=click.Choice([str(t) for t in DatasetManagerTask], case_sensitive=False),
    multiple=True,
    help='Dataset Task(s) to run in Work pipeline.'
)
@click.pass_context
def manager_run(ctx, certdb, task):
    """Runs Task pipeline for dataset(s) matching given filter(s)."""
    # Open CertDB
    try:
        certdb = CertFileDB(certdb, ctx.obj['cpu'])
    except ValueError:
        click.echo(
            'CertFileDB does not exist at {} yet, run "cevast certdb setup --help" for more information'.format(certdb)
        )
        ctx.exit(1)

    # Prepare parameters
    tasks = []
    for single in task:
        params = {}
        single = DatasetManagerTask[single]
        if single == DatasetManagerTask.COLLECT:
            pass
        elif single == DatasetManagerTask.UNIFY:
            params['certdb'] = certdb
        elif single == DatasetManagerTask.ANALYSE:
            params = _prepare_analyser_arg(certdb)

        tasks.append((single, params))

    ctx.obj['manager'].run(tasks)
    certdb.commit()

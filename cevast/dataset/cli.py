"""Group of CLI commands used for Dataset management and tasks.

   Entry point of `datasetmanager` and `datasetrepository` CLI.
"""

import os
from datetime import datetime
import click
from click import IntRange

from cevast.certdb import CertFileDB, CertFileDBReadOnly
from cevast.utils.logging import setup_cevast_logger, setup_cli_logger
from cevast.analysis import ChainValidator
from .dataset import DatasetRepository, DatasetSource, DatasetState, Dataset
from .manager_factory import DatasetManagerFactory, DatasetInvalidError
from .managers import DatasetManagerTask
from ..utils.enrichment_analyzer import EnrichmentAnalyzer

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


def _prepare_analyser_arg(certdb, reference_date):
    return {'analyser_cfg': {'certdb': certdb, 'reference_date': reference_date},
            'analyser': ChainValidator}


def __agregate(file):
    from collections import Counter

    with open(file) as f:
        header = f.readline().split(',')

        cntrs = [Counter() for _ in range(len(header) - 2)]  # methods only
        for line in f.readlines():
            els = [res.strip() for res in line.split(',')]
            for col, cntr in enumerate(cntrs, 1):
                cntr[els[col]] += 1

        return {header[idx].strip(): dict(cnt) for idx, cnt in enumerate(cntrs, 1)}


# -------------------------- DatasetRepository CLI --------------------------
@click.group('repository')
@click.argument('directory', type=click.Path(exists=True))
@click.pass_context
def dataset_repository_group(ctx, directory):
    """Gives access to Dataset Repository at <DIRECTORY>."""
    ctx.ensure_object(dict)
    if ctx.parent is None:  # Check if was called diretly via "datasetrepository" alias and should set up logger then
        setup_cevast_logger()
        setup_cli_logger()

    ctx.obj['repo'] = DatasetRepository(directory)


@dataset_repository_group.command('show')
@click.option('--source', '-s', 'source', type=click.Choice([str(t) for t in DatasetSource]), help='Dataset source to filter.')
@click.option('--state', '-s', type=click.Choice([str(t) for t in DatasetState]), help='Dataset State to filter.')
@click.option(
    '--date',
    '-d',
    callback=_validate_cli_filter_date,
    help='Dataset date to filter in format [YYYYMMDD] (only part of date can be set).',
)
@click.pass_context
def dataset_repository_show(ctx, source, state, date):
    """Show datasets in <DIRECTORY> matching given filter(s)."""
    click.echo(ctx.obj['repo'].dumps(dataset_id=date or '', source=source, state=state))


# ------------------------------- Dataset CLI -------------------------------
@click.group('manager')
@click.argument('directory', type=click.Path(exists=True))
@click.option(
    '--source', '-s', 'source', required=True, type=click.Choice([str(t) for t in DatasetSource]), help='Dataset source.'
)
@click.option(
    '--date',
    '-d',
    default=datetime.today().date().strftime(CLI_DATE_FORMAT),
    callback=_validate_cli_date,
    help='Dataset date in format [YYYY-mm-dd].',
)
@click.option(
    '--port', '-p', default=['443'], multiple=True, help='Dataset Port(s).'
)
@click.option('--cpu', type=int, help='Max Number of CPU cores to use.')
@click.version_option()
@click.pass_context
def manager_group(ctx, directory, source, date, port, cpu):
    """Gives access to specified Dataset management."""
    ctx.ensure_object(dict)

    if ctx.invoked_subcommand in {'stats', 'enrichments'}:
        ctx.obj['datasets'] = []
        for p in port:
            ctx.obj['datasets'].append(Dataset(directory, source, date.strftime('%Y%m%d'), p, 'csv' if ctx.invoked_subcommand == 'stats' else 'gz'))
        return

    if cpu is None:
        cpu = ctx.obj.get('cpu', os.cpu_count() - 1)  # Might be specified on top level command
    ctx.obj['cpu'] = cpu  # Pass it to subcommands

    if ctx.parent is None:  # Check if was called diretly via "manager" alias and should set up logger then
        setup_cevast_logger(process_id=cpu > 1)
        setup_cli_logger()

    try:
        manager = DatasetManagerFactory.get_manager(source)(repository=directory, date=date, ports=port, cpu_cores=cpu)
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
            'CertFileDB does not exist at {0} yet, run "cevast certdb {0} setup --help" for more information'.format(certdb)
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
@click.option(
    '--reference_date',
    '-r',
    default=datetime.today().date().strftime(CLI_DATE_FORMAT),
    callback=_validate_cli_date,
    help='Reference date for analysis in format [YYYY-mm-dd]. Default is today.',
)
@click.pass_context
def manager_analyse(ctx, certdb, reference_date):
    """Analyses dataset(s) matching given filter(s)."""
    try:
        certdb = CertFileDBReadOnly(certdb)
    except ValueError:
        click.echo(
            'CertFileDB does not exist at {0} yet, run "cevast certdb {0} setup --help" for more information'.format(certdb)
        )
        ctx.exit(1)

    analysed = ctx.obj['manager'].analyse(**_prepare_analyser_arg(certdb, reference_date))
    click.echo('Analysed Datasets: {}'.format(analysed))


@manager_group.command('runner')
@click.option(
    '--certdb',
    required=True,
    type=click.Path(exists=True),
    help='Path to CertDB where the certificates should be stored/read to/from.'
)
@click.option(
    '--reference_date',
    '-r',
    default=datetime.today().date().strftime(CLI_DATE_FORMAT),
    callback=_validate_cli_date,
    help='Reference date for analysis in format [YYYY-mm-dd]. Default is today.',
)
@click.option(
    '--task',
    '-t',
    type=click.Choice([str(t) for t in DatasetManagerTask], case_sensitive=False),
    multiple=True,
    help='Dataset Task(s) to run in Work pipeline.'
)
@click.pass_context
def manager_run(ctx, certdb, reference_date, task):
    """Runs Task pipeline for dataset(s) matching given filter(s)."""
    # Open CertDB
    try:
        certdb = CertFileDB(certdb, ctx.obj['cpu'])
    except ValueError:
        click.echo(
            'CertFileDB does not exist at {0} yet, run "cevast certdb {0} setup --help" for more information'.format(certdb)
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
            params = _prepare_analyser_arg(certdb, reference_date)

        tasks.append((single, params))

    collected, unified, analysed = ctx.obj['manager'].run(tasks)
    click.echo('Collected Datasets: {}'.format(collected))
    click.echo('Unified Datasets: {}'.format(unified))
    click.echo('Analysed Datasets: {}'.format(analysed))
    certdb.commit()


@manager_group.command('enrichments')
@click.option('--depth', type=IntRange(min=0), default=100, help='Enrichment depth (default is 100)')
@click.pass_context
def enrichments(ctx, depth):
    """Generate database enrichment stats."""
    for dataset in ctx.obj['datasets']:
        certs_file = dataset.full_path(DatasetState.COLLECTED, 'certs', True)
        hosts_file = dataset.full_path(DatasetState.COLLECTED, 'hosts', True)

        if certs_file and hosts_file:
            EnrichmentAnalyzer(certs_file, hosts_file, depth).run()


@manager_group.command('stats')
@click.option('--aggregate', '-a', is_flag=True, help='Shows aggregation statistics.')
@click.pass_context
def stats(ctx, aggregate):
    """Print statistics about dataset(s) matching given filter(s)."""
    for dataset in ctx.obj['datasets']:
        analysed = dataset.full_path(DatasetState.ANALYSED, check_if_exists=True)
        if not analysed:
            click.echo("Not found Dataset {}".format(str(dataset)).format('ANALYSED'))
            continue
        click.echo("Found Dataset {}".format(str(dataset)).format('ANALYSED'))
        if aggregate:
            for method, res in __agregate(analysed).items():
                click.echo("{:<10}: {}".format(method, sorted(res.items())))

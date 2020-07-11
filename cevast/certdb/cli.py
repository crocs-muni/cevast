"""Group of CLI commands used for CertDB management and access.

   Entry point of CertDB module CLI.

.. important::
   In development!
"""

import os
from datetime import datetime
import click
from cevast.certdb import CertFileDB, CertFileDBReadOnly

__author__ = 'Radim Podola'
__pdoc__ = {}


def validate_date(_, __, value):
    """Validates format of time"""
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        raise click.BadParameter('Date need to be in format YYYY-mm-dd')


@click.group('certdb')
@click.argument('storage', default='.', type=click.Path(exists=True))
@click.option('--cpu', type=int, help='Max Number of CPU cores to use.')
@click.option('--read-only/--read-write', default=True, help='Set read-only mode of CertDB.')
@click.version_option()
@click.pass_context
def certdb_group(ctx, storage, cpu, read_only):
    """Gives access to CertDB at <storage>."""
    ctx.ensure_object(dict)

    # IF SETUP subcommand was invoked, proceed
    if ctx.invoked_subcommand == certdb_setup.name:
        ctx.obj['storage'] = storage
        return

    try:
        if read_only:
            certdb = CertFileDBReadOnly(storage)
        else:
            if cpu is None:
                cpu = ctx.obj.get('cpu', os.cpu_count() - 1)
            certdb = CertFileDB(storage, cpu)
    except ValueError:
        click.echo('CertFileDB does not exist at {} yet, run "cevast certdb setup --help" for more information')
        ctx.exit(1)

    click.echo('Opening {} at {}'.format(certdb.__class__.__name__, storage))
    ctx.obj['certdb'] = certdb


@certdb_group.command('get')
@click.argument('certificate_id', nargs=-1)
@click.pass_context
def certdb_get(ctx, certificate_id):
    """Gets certificate(s) by given ID(s)."""
    for cert in certificate_id:
        print(ctx.obj['certdb'].get(cert))


@certdb_group.command('setup')
@click.pass_context
def certdb_setup(ctx):
    """Setups CertDB at <storage>."""
    CertFileDB.setup(ctx.obj['storage'], owner='cevast', desc='Cevast CertFileDB')


if __name__ == "__main__":
    certdb_group() # pylint: disable=E1120

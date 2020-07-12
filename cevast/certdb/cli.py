"""Group of CLI commands used for CertDB management and access.

   Entry point of `certdb` CLI.

.. important::
   In development! Only CertFileDB and read-only methods are supported now.

TODO: support type of CertDB
"""

import os
import click
from cevast.certdb import CertFileDB, CertFileDBReadOnly, CertNotAvailableError

__author__ = 'Radim Podola'


@click.group('certdb')
@click.argument('storage', type=click.Path(exists=True))
@click.option('--cpu', type=int, help='Max Number of CPU cores to use.')
@click.option('--read-only/--read-write', default=True, help='Set read-only mode of CertDB.')
@click.version_option()
@click.pass_context
def certdb_group(ctx, storage, cpu, read_only):
    """Gives access to CertDB at <STORAGE>."""
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
        click.echo(
            'CertFileDB does not exist at {} yet, run "cevast certdb setup --help" for more information'.format(storage)
        )
        ctx.exit(1)

    click.echo('Opening {} at {}'.format(certdb.__class__.__name__, storage))
    ctx.obj['certdb'] = certdb


@certdb_group.command('get')
@click.argument('certificate_id', nargs=-1)
@click.pass_context
def certdb_get(ctx, certificate_id):
    """Gets certificate(s) by given ID(s)."""
    for cert in certificate_id:
        try:
            click.echo(ctx.obj['certdb'].get(cert))
        except CertNotAvailableError:
            click.echo('Certificate {} does not exist in CertDB.'.format(cert))


@certdb_group.command('export')
@click.option('--dst', '-d', default='.', type=click.Path(exists=True), help='Target destination to export certificate(s).')
@click.argument('certificate_id', nargs=-1)
@click.pass_context
def certdb_export(ctx, certificate_id, dst):
    """Exports certificate(s) by given ID(s) to target directory."""
    for cert in certificate_id:
        try:
            click.echo('Certificate {} exported to {}'.format(cert, ctx.obj['certdb'].export(cert, dst)))
        except CertNotAvailableError:
            click.echo('Certificate {} does not exist in CertDB.'.format(cert))


@certdb_group.command('exist')
@click.argument('certificate_id', nargs=-1)
@click.pass_context
def certdb_exist(ctx, certificate_id):
    """Tests if certificate(s) exist (in case of multiple certificates, all must exist to return True)."""
    click.echo(ctx.obj['certdb'].exists_all(certificate_id))


@certdb_group.command('setup')
@click.pass_context
def certdb_setup(ctx):
    """Setups CertDB at <storage>."""
    CertFileDB.setup(ctx.obj['storage'], owner='cevast', desc='Cevast CertFileDB')

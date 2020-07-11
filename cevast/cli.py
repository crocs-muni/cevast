"""Entry point of CEVAST Command Line Interface."""

import os
import click
from cevast.utils.logging import setup_cevast_logger
from cevast.certdb import cli as certdb_cli


@click.group()
@click.option('--debug/--no-debug', default=False, help='DEBUG logging level will be turned on.')
@click.option('--cpu', default=os.cpu_count() - 1, type=int, help='Max Number of CPU cores to use.')
@click.version_option()
@click.pass_context
def cli(ctx, debug, cpu):
    """Cevast is a set of tools for collection and analysis of X.509 certificate datasets."""

    # based on parameters setup logger
    setup_cevast_logger(debug=debug, process_id=cpu > 1)

    click.echo('Debug mode is %s' % ('on' if debug else 'off'))
    click.echo('Starting')

    ctx.ensure_object(dict)
    ctx.obj['cpu'] = cpu


cli.add_command(certdb_cli.certdb_group)


if __name__ == "__main__":
    cli() # pylint: disable=E1120

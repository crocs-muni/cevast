"""Group of CLI commands used for certificate analysis.

   Entry point of `certanalyse` CLI.
"""

import click
from cevast.analysis.methods import get_all, get, show

__author__ = 'Radim Podola'


@click.command('analyse')
@click.option(
    '--method',
    '-m',
    type=click.Choice([str(m) for m in show()], case_sensitive=False),
    multiple=True,
    help='Analytical method to run on certificate(s) [all methods run by default].')
@click.option('--description', '-d', is_flag=True, help='Shows additional info about methods.')
@click.argument('certificate', nargs=-1)
def analysis_group(method, certificate, description):
    """Runs analytical methods on certificate chain c1 c2 cN (endpoint cert --> CA cert)."""
    if description:
        for info in show(True):
            click.echo(info)

    if certificate:
        if not method:
            method = show()
        for met in method:
            click.echo('{:<13}: {}'.format(met, get(met)(certificate)))

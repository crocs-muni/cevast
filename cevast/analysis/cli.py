"""Group of CLI commands used for certificate analysis.

   Entry point of `certanalyse` CLI.
"""

from datetime import datetime
import click
from cevast.analysis.methods import get, show
from cevast.dataset.cli import CLI_DATE_FORMAT, _validate_cli_date

__author__ = 'Radim Podola'


@click.command('analyse')
@click.option(
    '--method',
    '-m',
    type=click.Choice([str(m) for m in show()], case_sensitive=False),
    multiple=True,
    help='Analytical method to run on certificate(s) [all methods run by default].')
@click.option('--description', '-d', is_flag=True, help='Shows additional info about methods.')
@click.option(
    '--reference_date',
    '-r',
    default=datetime.today().date().strftime(CLI_DATE_FORMAT),
    callback=_validate_cli_date,
    help='Reference date for analysis in format [YYYY-mm-dd]. Default is today.',
)
@click.argument('certificate', nargs=-1)
def analysis_group(method, certificate, description, reference_date):
    """Runs analytical methods on certificate chain c1 c2 cN (endpoint cert --> CA cert)."""
    if description:
        for info in show(True):
            click.echo(info)

    if certificate:
        if not method:
            method = show()

        validation_method_arguments = {"reference_time": int(reference_date.strftime("%s"))}

        for met in method:
            click.echo('{:<15}: {}'.format(met, get(met)(list(certificate), **validation_method_arguments)))

"""
This module provide functions supporting work with certificates.
"""
import textwrap

__author__ = 'Radim Podola'


def validate_PEM(cert: str) -> bool:
    """Simply verify the PEM certificate format"""
    return cert.startswith('-----BEGIN CERTIFICATE-----\n') and cert.endswith('\n-----END CERTIFICATE-----')


def BASE64_to_PEM(cert: str) -> str:
    """Convert a raw BASE64 encoded certificate to PEM format"""
    return '-----BEGIN CERTIFICATE-----' + '\n' + "\n".join(textwrap.wrap(cert, width=64)) + '\n' + '-----END CERTIFICATE-----'


def make_PEM_filename(cert_id: str) -> str:
    """Create a filename for PEM certificate"""
    return cert_id + '.pem'

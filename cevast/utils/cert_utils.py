"""
This module provide functions supporting work with certificates.
"""

__author__ = 'Radim Podola'


def validate_PEM(cert: str) -> bool:
    return cert.startswith('-----BEGIN CERTIFICATE-----\n') and \
           cert.endswith('\n-----END CERTIFICATE-----')


def BASE64_to_PEM(cert: str) -> str:
    return '-----BEGIN CERTIFICATE-----' + '\n' + cert + '\n' + '-----END CERTIFICATE-----'


def make_PEM_filename(id: str) -> str:
    return id + '.pem'

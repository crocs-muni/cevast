from . methods import validate_chain_open_ssl, validate_chain_python_ssl

def validator(chain: list, cfg: dict):
    results = {}
    if cfg.get('validate_chain_open_ssl', False):
        results['validate_chain_open_ssl'] = validate_chain_open_ssl(chain)

    if cfg.get('validate_chain_python_ssl', False):
        results['validate_chain_python_ssl'] = validate_chain_python_ssl(chain)

    return results

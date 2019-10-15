"""
es2es
=====

Generates the required command line commands for transferring data between 
elasticsearch instances.

The commands require elasticdump to be installed via npm.
"""

from nesta.core.orms.orm_utils import get_config

ENDPOINT = "{url}:{port}/{index}"
ESDUMP = "elasticdump --input={_input} --output={_output} --type={_type} --limit=2000"

def _endpoint(url, index, port):
    """Formulates the endpoint URL"""
    return ENDPOINT.format(url=url, index=index, port=port)

def esdump_str(label, in_host, _type):
    """Formulates the elasticdump command, by printing to screen.
    
    Args:
        label (str): Label in the elasticsearch config file to look under.
        in_host (str): The URL for the input host.
        _type (str): One of 'mapping', 'setting' and 'data', corresponding to what you 
                     would like to transfer between ES instances.
    """
    config = get_config('elasticsearch.config', label)
    _input = _endpoint(in_host, config['index'], config['port'])
    _output = _endpoint(config['host'], config['index'], config['port'])
    print(ESDUMP.format(_input=_input, _output=_output, _type=_type))

if __name__ == "__main__":
    import sys
    esdump_str(*sys.argv[1:], _type='settings')
    esdump_str(*sys.argv[1:], _type='mapping')
    esdump_str(*sys.argv[1:], _type='data')

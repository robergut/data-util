"""
    Loads the properties to connect to a database from a file
"""

import yaml

from configparser import ConfigParser

def read_yaml(cfg_file='conf.yaml'):
    with open(cfg_file) as f:
        data = yaml.safe_load(f)
    return data


def load_config(filename, section):
    """ 
        The following config() function reads a `.ini` file and
        returns the connection parameters as a dictionary. This function will
        be imported in to the main python script.
    """

    config = {}
    parser = ConfigParser()
    parser.read(filename)

    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception(f"Configuration [{section}] not found in '{filename}'")

    return config

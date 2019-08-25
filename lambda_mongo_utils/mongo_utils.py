#!/usr/bin/env python3

'''
Miscellaneous utilities to interface with Mongo.
'''

import logging
import os
from pathlib import Path
import re
import urllib.parse
import tarfile
import tempfile


LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(os.getenv('LOG_LEVEL', 'INFO'))


URL_DOWNLOAD_BASE = (
    'http://downloads.mongodb.org/linux/mongodb-linux-x86_64-amazon2-v'
)
AVAILABLE_MONGO_UTILS = [
    'bsondump',
    'install_compass',
    'mongo',
    'mongod',
    'mongodump',
    'mongoexport',
    'mongofiles',
    'mongoimport',
    'mongoreplay',
    'mongorestore',
    'mongos',
    'mongostat',
    'mongotop',
]


def parse_uri(uri):
    '''
    Parses a Mongo connection URI, returnining a dictionary with at least
    the following key:

    - db: name of the database
    - host: list of host_address:port
    - user: connection username
    - pwd: connection username

    Other query string parameters, if any, are also available as keys.

    >>> parse_uri('mongodb://localhost') == {
    ...     'host': 'localhost',
    ...     'db': '',
    ...     'user': '',
    ...     'pwd': '',
    ... }
    True

    >>> parse_uri('mongodb://USER:PWD@HOST1,HOST2:27017/db?replicaSet=r0') == {
    ...     'host': 'HOST1,HOST2:27017',
    ...     'db': 'db',
    ...     'user': 'USER',
    ...     'pwd': 'PWD',
    ...     'replicaSet': 'r0',
    ... }
    True
    '''

    rx = (
        r'^mongodb://((?P<user>\w+):(?P<pwd>\w+)@)?'
        r'(?P<host>[0-9a-zA-Z_:,.-]+)(/(?P<db>\w*)?)?'
    )
    default_keys = ['db', 'host', 'user', 'pwd']

    query_params = {}
    uri, sep, query_params_spec = uri.partition('?')
    if sep and query_params_spec:
        query_params = {
            k: v[0]
            for k, v in urllib.parse.parse_qs(query_params_spec).items()
            if v and v[0]
        }

    result = query_params.copy()
    result.update(re.match(rx, uri).groupdict())

    for part in default_keys:
        result[part] = result.get(part) or ''

    return result


def get_cmd_args(uri):
    '''
    Builds the list of command-line arguments to connect to the database
    via Mongo utils.

    >>> get_cmd_args('mongodb://localhost/dblocal')
    ['--authenticationDatabase', 'dblocal', '--host', 'localhost']

    >>> get_cmd_args('mongodb://user:pwd@h1,h2/db_remote?replicaSet=rs0') == [
    ...     '--authenticationDatabase', 'db_remote',
    ...     '--host', 'rs0/h1,h2',
    ...     '--user', 'user',
    ...     '--password', 'pwd'
    ... ]
    True
    '''

    parts = parse_uri(uri)
    args = []

    args += ['--authenticationDatabase', parts['db'] or 'admin']

    if parts.get('replicaSet'):
        args += ['--host', f"{parts['replicaSet']}/{parts['host']}"]
    else:
        args += ['--host', parts['host']]

    if parts.get('user'):
        args += ['--user', parts['user'], '--password', parts['pwd']]

    return args


def download_utils(dest='/tmp/bin', version='4.0-latest', utils=None):
    '''
    Downloads a Mongo version and extracts the specified binaries.

    Args:
    - dest: destination directory (created if it doesn't exist)
    - version: version of Mongo to be downloaded (default: 4.0)
    - utils: utils to be downloaded (default: ['mongo', 'mongodump'])

    Returns
        {MONGO_UTIL_NAME: PATH}

    The names for the versions can be found in the URL
    https://www.mongodb.org/dl/linux/x86_64-amazon2
    '''
    url = f'{URL_DOWNLOAD_BASE}{version}.tgz'
    dest = Path(dest)
    utils = ['mongo', 'mongodump'] if utils is None else utils

    if dest.exists() and not dest.is_dir():
        raise Exception('Destination is not a directory')

    dest.mkdir(parents=True, exist_ok=True)
    utils_to_return = {}

    with tempfile.TemporaryDirectory() as tmpdir:
        temp_tgz = Path(tmpdir) / 'mongo.tgz'

        LOGGER.info('Downloading %s', url)
        urllib.request.urlretrieve(url, temp_tgz)
        LOGGER.info('Downloaded to %s', temp_tgz)

        import shutil
        shutil.copyfile(temp_tgz, '/home/diogenes/temp.tgz')

        with tarfile.open(temp_tgz) as tar:
            member_names = tar.getnames()

            for util in utils:
                util_dest_path = Path(dest) / util

                try:
                    member_name = [
                        m for m in member_names
                        if m.endswith(f'bin/{util}')
                    ][0]
                except IndexError:
                    raise ValueError(f'No such util {util} in the file')
                else:
                    LOGGER.info('Extracting %s to %s', util, util_dest_path)
                    with util_dest_path.open('wb') as fp:
                        fp.write(tar.extractfile(member_name).read())

                    LOGGER.info('Adding chmod +x to %s', util_dest_path)
                    util_dest_path.chmod(0o755)

                    utils_to_return[util] = str(util_dest_path)

            if not utils:
                LOGGER.info('No util to extract')

    return utils_to_return

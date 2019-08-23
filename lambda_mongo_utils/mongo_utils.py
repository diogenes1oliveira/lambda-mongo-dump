#!/usr/bin/env python3

'''
Miscellaneous utilities to interface with Mongo.
'''

import re
import urllib.parse


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

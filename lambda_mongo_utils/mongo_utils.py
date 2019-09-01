#!/usr/bin/env python3

'''
Miscellaneous utilities to interface with Mongo.
'''

from contextlib import contextmanager
from dataclasses import dataclass
import logging
import os
from pathlib import Path
import re
from shlex import split as shell_split
import subprocess
import urllib.parse
import tarfile
import tempfile
import time
from typing import (
    Any,
    BinaryIO,
    ContextManager,
    Iterable,
    Mapping,
    NamedTuple,
    Optional,
)

from bson.objectid import ObjectId

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


@dataclass
class MongoStats:
    collection: str = None
    db: str = None
    num_docs: int = None
    time: float = None
    duplicated_ids: Iterable[str] = None

    @contextmanager
    def measure(self):
        t0 = time.time()
        yield
        self.time = time.time() - t0


class MongoDumpOutput(NamedTuple):
    stream: BinaryIO
    stats: MongoStats


def parse_uri(uri: str) -> Mapping[str, Optional[str]]:
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


def get_cmd_args(uri: str) -> Iterable[str]:
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


def download_utils(
    dest: str = '/tmp/bin',
    version: str = '4.0-latest',
    utils: str = None,
):
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


@contextmanager
def mongo_dump(
    uri: str,
    collection: str,
    db: str = None,
    query: Mapping[str, Any] = None,
    buffer_size=None,
    count=True,
    cmd_prefix: str = '',
) -> ContextManager[MongoDumpOutput]:
    '''
    Executes mongodump, yielding a stream to read its output.

    Args:
    - uri: Mongo connection string
    - collection: name of the collection to be dumped
    - db: name of the database (defaults to the one in the URI or 'admin')
    - query: query to select the documents to be dumped
    - buffer_size: size of the buffer for the stdout (default: 10MB)
    - count: count the number of documents
    - cmd_prefix: prefix to be added to the mongodump base command

    Yields:
        MongoDumpOutput
    '''
    buffer_size = buffer_size or 10_000_000

    parts = parse_uri(uri)
    db = db or parts.get('db', 'admin')
    stats = MongoStats(db=db, collection=collection)

    args = shell_split(cmd_prefix + 'mongodump')
    args += get_cmd_args(uri)
    args += [
        '--db', db,
        '--collection', collection,
        '--archive', '--gzip',
    ]
    try:
        process = subprocess.Popen(
            args,
            errors=None,
            stdout=subprocess.PIPE,
            stderr=(subprocess.PIPE if count else subprocess.DEVNULL),
            text=None,
            encoding=None,
            universal_newlines=None,
            bufsize=buffer_size,
        )

        with stats.measure():
            yield MongoDumpOutput(
                stream=process.stdout,
                stats=stats,
            )
        _, stderr = process.communicate(timeout=1.0)
        stderr = stderr.decode('utf-8') if stderr else ''

        if process.returncode != 0:
            LOGGER.error(stderr)
            raise Exception(
                f'mongodump exited with error code = {process.returncode}')

        m = re.search(
            f'done dumping {db}.{collection} ' +
            r'\((?P<num>\d+) documents\)',
            stderr,
            re.MULTILINE,
        )
        if m:
            stats.num_docs = int(m.group('num'))

    finally:
        process.terminate()


def mongo_restore(
    stream: BinaryIO,
    uri: str,
    collection: str,
    db: str = None,
    buffer_size=None,
    drop=False,
    cmd_prefix='',
) -> MongoStats:
    '''
    Executes mongorestore, restoring a previously gzipped dump from the given
    stream.

    Args:
    - stream: stream to read from
    - uri: Mongo connection string
    - collection: name of the collection to be restored
    - db: name of the database (defaults to the one in the URI or 'admin')
    - buffer_size: size of each chunk to be read (default: 10MB)
    - drop: drop current collection
    - cmd_prefix: prefix to be added to the mongorestore base command

    Yields:
        MongoStats
    '''
    buffer_size = buffer_size or 10_000_000

    parts = parse_uri(uri)
    db = db or parts.get('db', 'admin')
    stats = MongoStats(db=db, collection=collection)

    args = shell_split(cmd_prefix + 'mongorestore')
    args += get_cmd_args(uri)
    if drop:
        args += ['--drop']
    args += [
        '--archive', '--gzip',
        '--nsInclude', '*.*',
        '--nsFrom', '$db$.$col$',
        '--nsTo', f'{db}.{collection}',
    ]

    with stats.measure():
        process = subprocess.run(
            args,
            errors=None,
            stdin=stream,
            text=True,
            check=True,
            bufsize=buffer_size,
            capture_output=True,
        )

        num_match = re.search(
            f'finished restoring {db}.{collection} ' +
            r'\((?P<num>\d+) documents\)',
            process.stderr,
            re.MULTILINE,
        )
        stats.num_docs = int(num_match.group('num')) if num_match else None

        dup_match = re.findall(
            r"_id_ dup key: \{ : ObjectId\('(?P<id>[0-9a-fA-F]+)'\) \}",
            process.stderr,
            re.MULTILINE,
        )
        stats.duplicated_ids = [
            ObjectId(id) for id in dup_match if id
        ]
        stats.num_docs -= len(stats.duplicated_ids)

    return stats

import io
import logging
import os
import re
import shutil
import subprocess
import tarfile
import uuid
import urllib.request

from hypothesis import given, strategies
from pymongo import MongoClient
import pytest
from unittest.mock import MagicMock

from lambda_mongo_utils import mongo_utils
from .common_utils import wait_for_mongo_to_be_up


LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(os.getenv('LOG_LEVEL', 'INFO'))


def to_utf8(s):
    if hasattr(s, 'decode'):
        return s.decode('utf-8')
    else:
        return str(s)


def mock_mongo_file(version, dest):
    with tarfile.open(dest, 'w') as tar:
        for util in mongo_utils.AVAILABLE_MONGO_UTILS:
            cmd = f'echo -n {util} {version}'.encode('ascii')
            tar_info = tarfile.TarInfo(f'mongo-xxx/bin/{util}')
            tar_info.size = len(cmd)

            tar.addfile(tar_info, io.BytesIO(cmd))


@given(
    utils=strategies.lists(
        strategies.sampled_from(mongo_utils.AVAILABLE_MONGO_UTILS),
        unique=True,
    ),
)
def test_download_utils(tmp_path, monkeypatch, utils):

    versions = {
        '4.2-latest': 'http://downloads.mongodb.org/linux/mongodb-linux-x86_64-amazon2-v4.2-latest.tgz',  # noqa: E501
        '4.0-latest': 'http://downloads.mongodb.org/linux/mongodb-linux-x86_64-amazon2-v4.0-latest.tgz',  # noqa: E501
        '4.2.0': 'http://downloads.mongodb.org/linux/mongodb-linux-x86_64-amazon2-v4.2.0.tgz',            # noqa: E501
    }

    for version, version_url in versions.items():
        mock = MagicMock()
        monkeypatch.setattr(urllib.request, 'urlretrieve', lambda url, dest: (
            mock_mongo_file(version, dest), mock(url),
        ))
        dest = tmp_path / 'tmp-download-utils' / version
        shutil.rmtree(dest, ignore_errors=True)

        r = mongo_utils.download_utils(dest=dest, version=version, utils=utils)
        mock.assert_called_once_with(version_url)

        util_paths = dest.glob('*')
        assert set(u.name for u in util_paths) == set(utils)

        for util, util_path in r.items():
            process = subprocess.run(
                ['sh', '-c', util_path],
                capture_output=True,
                check=True,
                text=True,
            )
            assert f'{util} {version}' == process.stdout

        shutil.rmtree(dest, ignore_errors=True)
        with pytest.raises(ValueError):
            mongo_utils.download_utils(
                dest=dest, version=version, utils=['non-existing-util'])

        dest = dest / f'binary--{uuid.uuid4()}'
        with dest.open('w') as fp:
            fp.write('testing...')

        with pytest.raises(Exception) as e:
            mongo_utils.download_utils(dest=dest, version=version)

        assert re.search('not a directory', str(e.value))


def test_mongo_dump_and_restore(docker_container, tmp_path):
    # Dummy data insertion
    docs = [
        {'name': 'col1doc1'},
        {'name': 'col1doc2'},
        {'name': 'col1doc3'},
    ]
    inserted_doc_ids = None
    port = 27020
    client = MongoClient(f'mongodb://localhost:{port}')
    uri = 'mongodb://localhost/tmpdb'
    dump_path = str(tmp_path / 'dump1.tgz')

    with docker_container('mongo:4.0', ports={'27017/tcp': str(port)}, appdir=str(tmp_path)) as container:  # noqa: E501
        wait_for_mongo_to_be_up(container)
        cmd_prefix = f'docker exec -i {container.id} '
        inserted_doc_ids = client.db1['col1'].insert_many(docs).inserted_ids

        # Get a dump after inserting the documents
        with mongo_utils.mongo_dump(cmd_prefix=cmd_prefix, uri=uri, collection='col1', db='db1') as (stream, stats):  # noqa: E501
            with open(dump_path, 'wb') as fp:
                fp.write(stream.read())

        assert stats.num_docs == 3

        # Doesn't count the number of docs if requested not to
        with mongo_utils.mongo_dump(cmd_prefix=cmd_prefix, uri=uri, collection='col1', db='db1', count=False) as (_, stats):  # noqa: E501
            pass
        assert not stats.num_docs

        # Test if a dummy falsey command throws
        with pytest.raises(Exception) as exc:
            with mongo_utils.mongo_dump(cmd_prefix=cmd_prefix + ' false ', uri=uri, collection='col1', db='db1') as _:  # noqa: E501
                pass
        assert re.search('exited with error code', str(exc))

    with docker_container('mongo:4.0', ports={'27017/tcp': str(port)}, appdir=str(tmp_path)) as container:  # noqa: E501
        wait_for_mongo_to_be_up(container)

        def restore_dump(**kwargs):
            with open(dump_path, 'rb') as fp:
                return mongo_utils.mongo_restore(
                    stream=fp,
                    cmd_prefix=f'docker exec -i {container.id} ',
                    uri=uri,
                    collection='col2',
                    db='db2',
                    **kwargs,
                )

        # Insert one document and check if it wasn't overwritten
        col = MongoClient(f'mongodb://localhost:{port}').db2['col2']
        col.insert_one({
            '_id': inserted_doc_ids[0],
            'name': 'test',
        })
        stats = restore_dump()
        assert {d['name'] for d in col.find()} == {
            'test', 'col1doc2', 'col1doc3',
        }
        assert stats.num_docs == 2

        # Checking if duplicated docs are properly returned
        col.drop()
        col.insert_one({
            '_id': inserted_doc_ids[0],
            'name': 'new doc 1',
        })
        col.insert_one({'name': 'new doc 2'})
        stats = restore_dump()

        assert stats.duplicated_ids == [inserted_doc_ids[0]]

        r = col.delete_many({'_id': {'$in': stats.duplicated_ids}})
        LOGGER.warning(r.raw_result)
        stats = restore_dump()
        assert stats.num_docs == 1
        assert set(stats.duplicated_ids) == set(inserted_doc_ids[1:])
        assert {d['name'] for d in col.find()} == {
            'col1doc1', 'new doc 2', 'col1doc2', 'col1doc3',
        }

        # Now drop the collection
        col.insert_one({'name': 'new doc'})
        stats = restore_dump(drop=True)
        assert {d['name'] for d in col.find()} == {
            'col1doc1', 'col1doc2', 'col1doc3',
        }
        assert stats.num_docs == 3

        with pytest.raises(Exception) as exc:
            restore_dump(cmd_prefix=f'docker exec -i {container.id} false ')
        assert re.search('exited with error code', str(exc))

import logging
import os
import re
from uuid import uuid4

from pymongo import MongoClient
import pytest


from lambda_mongo_utils import backup_utils
from .common_utils import (
    fake_docs,
    wait_for_mongo_to_be_up,
)

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(os.getenv('LOG_LEVEL', 'INFO'))


def unique_name(prefix='name_'):
    return prefix + str(uuid4()).replace('-', '')


def test_mongo_dump_and_restore_s3(s3, temp_bucket, docker_container):
    key = unique_name('test_mongo_dump_to_s3_')
    port = '27020'
    external_uri = f'mongodb://localhost:{port}/'
    container_uri = 'mongodb://localhost:27017/'
    docs = fake_docs()

    with docker_container('mongo:4.0', ports={'27017/tcp': port}) as container:
        wait_for_mongo_to_be_up(container)
        coldump = MongoClient(external_uri).dbdump.coldump

        inserted_ids = coldump.insert_many(docs).inserted_ids

        with pytest.raises(Exception) as exc:
            backup_utils.mongo_dump_to_s3(
                uri=container_uri,
                collection='coldump',
                db='dbdump',
                bucket=temp_bucket,
                key=key,
                cmd_prefix=f'false ',
            )

        LOGGER.exception(exc)
        assert re.search('exited with error code', str(exc))
        assert not s3.list_multipart_uploads(
            Bucket=temp_bucket).get('Uploads', [])

        dump_stats = backup_utils.mongo_dump_to_s3(
            uri=container_uri,
            collection='coldump',
            db='dbdump',
            bucket=temp_bucket,
            key=key,
            cmd_prefix=f'docker exec -i {container.id} '
        )

        assert dump_stats.num_docs == len(docs)
        assert s3.get_object(
            Bucket=temp_bucket, Key=key)['ContentLength'] == dump_stats.size

        restore_stats = backup_utils.mongo_restore_from_s3(
            uri=container_uri,
            collection='colrestore',
            db='dbrestore',
            bucket=temp_bucket,
            key=key,
            cmd_prefix=f'docker exec -i {container.id} '
        )

        assert restore_stats.num_docs == dump_stats.num_docs

        colrestore = MongoClient(external_uri).dbrestore.colrestore
        restored_ids = [doc['_id'] for doc in colrestore.find()]

        assert set(restored_ids) == set(inserted_ids)

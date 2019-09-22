#!/usr/bin/env python3

'''
Miscellaneous utilities to connect Mongo backups to AWS S3.
'''
from contextlib import closing, contextmanager
from dataclasses import dataclass
import time
from typing import (
    Any,
    Mapping,
)

import boto3

from .mongo_utils import mongo_dump, mongo_restore, parse_uri
from .multipart_upload import S3MultipartUpload


@dataclass
class BackupStats:
    bucket: str
    collection: str
    db: str
    key: str
    num_docs: int = None
    time: float = None
    size: int = None

    @contextmanager
    def measure(self):
        t0 = time.time()
        yield
        self.time = time.time() - t0


def mongo_dump_to_s3(
    uri: str,
    collection: str,
    bucket: str,
    key: str,
    db: str = None,
    chunk_size: int = None,
    buffer_size: int = None,
    query: Mapping[str, Any] = None,
    cmd_prefix: str = None,
) -> BackupStats:
    '''
    Dumps a Mongo collection directly to S3.

    Multi-part upload is used by default.

    Args:
    - uri: Mongo connection URI
    - collection: name of the connection to be dumped
    - bucket: name of the S3 bucket to store the dump
    - key: S3 key under which the dump will be stored
    - db: name of the database where the desired collection is stored. Defaults
    to the one in the URI or 'admin'
    - chunk_size: size of each chunk to be sent to S3
    - buffer_size: size of the output buffer for mongodump
    - query: JSON object to restrict the dumped documents
    - cmd_prefix: prefix to be added to the command-line Mongo tools
    '''
    uri_parts = parse_uri(uri)
    db = db or uri_parts.get('db', 'admin')
    stats = BackupStats(
        db=db,
        collection=collection,
        bucket=bucket,
        key=key,
    )
    mpu = S3MultipartUpload(
        bucket=bucket, key=key, chunk_size=chunk_size, buffer_size=buffer_size,
    )
    mpu.abort_all()
    mpu_id = mpu.create()

    try:
        with stats.measure(), mongo_dump(
            uri=uri,
            collection=collection,
            db=db,
            query=query,
            buffer_size=buffer_size,
            cmd_prefix=cmd_prefix,
        ) as (stream, dump_stats):
            parts, size = mpu.upload_from_stream(mpu_id, stream)
    except Exception:
        mpu.abort_all()
        raise
    else:
        mpu.complete(mpu_id, parts)

    stats.size = size
    stats.num_docs = dump_stats.num_docs

    return stats


def mongo_restore_from_s3(
    uri: str,
    collection: str,
    bucket: str,
    key: str,
    db: str = None,
    chunk_size: int = None,
    cmd_prefix: str = None,
) -> BackupStats:
    '''
    Dumps a Mongo collection directly to S3.

    Multi-part upload is used by default.

    Args:
    - uri: Mongo connection URI
    - collection: name of the connection to be restored
    - bucket: name of the S3 bucket to fetch the dump
    - key: S3 key under which the dump is stored
    - db: destination database. Defaults to the one in the URI or 'admin'
    - chunk_size: size of each chunk to be read from S3
    - cmd_prefix: prefix to be added to the command-line Mongo tools
    '''
    uri_parts = parse_uri(uri)
    db = db or uri_parts.get('db', 'admin')
    stats = BackupStats(
        db=db,
        collection=collection,
        bucket=bucket,
        key=key,
    )

    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)

    with stats.measure():
        stream = obj['Body']._raw_stream
        with closing(stream):
            restore_stats = mongo_restore(
                stream=stream,
                uri=uri,
                collection=collection,
                db=db,
                buffer_size=chunk_size,
                cmd_prefix=cmd_prefix,
            )

    stats.size = obj['ContentLength']
    stats.num_docs = restore_stats.num_docs
    return restore_stats

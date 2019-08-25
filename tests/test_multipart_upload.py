import io
import os
import shlex
import subprocess
import sys
from unittest.mock import MagicMock
import uuid


from hypothesis import given, strategies
import pytest

from lambda_mongo_utils.multipart_upload import (
    S3MultipartUpload,
    main,
)


@pytest.fixture
def temp_bucket(s3):
    bucket_name = f'temp-bucket-{uuid.uuid4()}'
    s3.create_bucket(Bucket=bucket_name)
    return bucket_name


@pytest.fixture(scope='function')
def temp_content(tmp_path):
    def inner(size):
        content = os.urandom(size)
        content_path = tmp_path / f'{uuid.uuid4()}.dat'
        with content_path.open('wb') as fp:
            fp.write(content)

        return content, str(content_path)

    return inner


def test_abort_multipart_upload(s3, temp_bucket, monkeypatch):
    mpu1 = S3MultipartUpload(bucket=temp_bucket, key='key1')
    mpu2 = S3MultipartUpload(bucket=temp_bucket, key='key2')

    # No upload must exist yet
    assert mpu1.abort_all() == []

    mpu1_id = mpu1.create()

    # Returns correctly the aborted multipart uploads
    assert mpu1.abort_all() == [mpu1_id]

    # Check if it isn't getting confused by different keys
    assert mpu2.abort_all() == []

    monkeypatch.setattr(mpu1.s3, 'list_multipart_uploads', MagicMock(
        return_value={'Uploads': [
            {'Key': 'key1', 'UploadId': 'my-non-existing-upload'}]}
    ))

    # No remaining upload
    assert mpu1.abort_all() == []


@given(
    capture=strategies.booleans(),
)
def test_upload_single_part(s3, temp_bucket, tmp_path, temp_content, capture):
    '''
    Generating a temporary file with unique random values to simulate
    the upload of a single-parted upload
    '''
    content, content_path = temp_content(S3MultipartUpload.PART_MINIMUM)

    key = 'my-key'

    mpu = S3MultipartUpload(
        bucket=temp_bucket,
        key=key,
        chunk_size=S3MultipartUpload.PART_MINIMUM,
    )
    mpu_id = mpu.create()

    if capture:
        capture_id = str(uuid.uuid4())
        cmd_args = [
            'sh',
            '-c',
            f'echo {capture_id} >&2; cat {shlex.quote(str(content_path))}'
        ]
    else:
        capture_id = ''
        cmd_args = ['cat', str(content_path)]

    parts, stderr, size = mpu.upload_from_stdout(
        mpu_id, cmd_args, capture_stderr=capture,
    )
    mpu.complete(mpu_id, parts)

    if capture:
        assert capture_id in stderr.decode('utf-8')
    assert len(parts) == 1
    assert size == S3MultipartUpload.PART_MINIMUM

    response = s3.get_object(
        Bucket=temp_bucket,
        Key=key,
    )

    assert response['Body'].read() == content


@given(
    num_chunks=strategies.integers(1, 3),
    num_parts=strategies.integers(1, 3),
    half=strategies.booleans(),
)
def test_upload_multiple_parts(
    s3,
    temp_bucket,
    temp_content,
    tmp_path,
    num_chunks,
    num_parts,
    half,
):
    '''
    Generating a temporary file with unique random values to simulate
    the upload of a multiple-parted upload
    '''
    key = f'temp-key-{uuid.uuid4()}'
    content, content_path = temp_content(
        num_chunks * num_parts * S3MultipartUpload.PART_MINIMUM +
        (S3MultipartUpload.PART_MINIMUM // 2 if half else 0)
    )

    mpu = S3MultipartUpload(
        bucket=temp_bucket,
        key=key,
        chunk_size=num_chunks * S3MultipartUpload.PART_MINIMUM,
        buffer_size=2 * S3MultipartUpload.PART_MINIMUM,
    )
    mpu_id = mpu.create()
    parts, _, size = mpu.upload_from_stdout(mpu_id, ['cat', str(content_path)])
    mpu.complete(mpu_id, parts)

    assert size == len(content)

    response = s3.get_object(
        Bucket=temp_bucket,
        Key=key,
    )

    assert response['Body'].read() == content


def test_failed_command(s3, temp_bucket, temp_content, monkeypatch):
    key = 'temp-key-failed-command'
    _, content_path = temp_content(S3MultipartUpload.PART_MINIMUM)

    mpu = S3MultipartUpload(
        bucket=temp_bucket,
        key=key,
    )
    mpu_id = mpu.create()

    with pytest.raises(subprocess.CalledProcessError):
        mpu.upload_from_stdout(mpu_id, ['false'])

    mpu_id = mpu.create()

    monkeypatch.setattr(subprocess.Popen, 'wait', MagicMock(
        side_effect=subprocess.TimeoutExpired(0, 'cmd')
    ))
    with pytest.raises(subprocess.TimeoutExpired):
        mpu.upload_from_stdout(mpu_id, ['cat', content_path])


def test_command_args(s3, temp_bucket, temp_content, tmp_path, monkeypatch):
    key = f'temp-key-{uuid.uuid4()}'
    content, content_path = temp_content(S3MultipartUpload.PART_MINIMUM)

    args = [
        '--bucket', temp_bucket,
        '--key', key,
        'cat', content_path,
    ]

    monkeypatch.setattr(sys, 'argv', ['myname'] + args)
    main()

    response = s3.get_object(
        Bucket=temp_bucket,
        Key=key,
    )
    assert response['Body'].read() == content

    result = subprocess.run([
        'multi-part-upload-from-stdout',
        '--bucket', temp_bucket,
        '--key', key,
        'sh', '-c', 'exit 299',
    ])
    assert result.returncode in [1, 299]


@given(
    size=strategies.integers(1, 2 * S3MultipartUpload.PART_MINIMUM),
)
def test_upload_from_stream(s3, temp_bucket, size):
    content = os.urandom(size)
    stream = io.BytesIO(content)
    key = f'test-upload-from-stream-{size}'

    mpu = S3MultipartUpload(bucket=temp_bucket, key=key)

    mpu_id = mpu.create()
    parts, size = mpu.upload_from_stream(mpu_id, stream)
    mpu.complete(mpu_id, parts)

    response = s3.get_object(Bucket=temp_bucket, Key=key)
    assert response['Body'].read() == content

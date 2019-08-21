import os
import uuid

from hypothesis import given, strategies
import pytest

from lambda_mongo_utils.multipart_upload import S3MultipartUpload


@pytest.fixture
def temp_bucket(s3):
    bucket_name = f'temp-bucket-{uuid.uuid4()}'
    s3.create_bucket(Bucket=bucket_name)
    return bucket_name


def test_abort_multipart_upload(s3, temp_bucket):
    mpu1 = S3MultipartUpload(bucket=temp_bucket, key='key1')
    mpu2 = S3MultipartUpload(bucket=temp_bucket, key='key2')

    # No upload must exist yet
    assert mpu1.abort_all() == []

    mpu1_id = mpu1.create()

    # Returns correctly the aborted multipart uploads
    assert mpu1.abort_all() == [mpu1_id]

    # Check if it isn't getting confused by different keys
    assert mpu2.abort_all() == []


def test_upload_single_part(s3, temp_bucket, tmp_path):
    '''
    Generating a temporary file with unique random values to simulate
    the upload of a single-parted upload
    '''
    content = os.urandom(S3MultipartUpload.PART_MINIMUM)
    content_path = tmp_path / 'content.dat'
    with content_path.open('wb') as fp:
        fp.write(content)

    key = 'my-key'

    mpu = S3MultipartUpload(
        bucket=temp_bucket,
        key=key,
        chunk_size=S3MultipartUpload.PART_MINIMUM,
    )
    mpu_id = mpu.create()
    parts, _, size = mpu.upload_from_stdout(mpu_id, ['cat', str(content_path)])
    mpu.complete(mpu_id, parts)

    assert len(parts) == 1
    assert size == S3MultipartUpload.PART_MINIMUM

    response = s3.get_object(
        Bucket=temp_bucket,
        Key=key,
    )

    assert response['Body'].read() == content


@given(
    num_chunks=strategies.integers(1, 5),
    num_parts=strategies.integers(2, 3),
)
def test_upload_multiple_parts(s3, temp_bucket, tmp_path, num_chunks, num_parts):
    '''
    Generating a temporary file with unique random values to simulate
    the upload of a multiple-parted upload
    '''
    key = f'temp-key-{uuid.uuid4()}'

    content = os.urandom(num_chunks * num_parts)
    content_path = tmp_path / f'{key}.dat'
    with content_path.open('wb') as fp:
        fp.write(content)

    mpu = S3MultipartUpload(
        bucket=temp_bucket,
        key=key,
        chunk_size=num_chunks * S3MultipartUpload.PART_MINIMUM,
        buffer_size=S3MultipartUpload.PART_MINIMUM * 2,
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

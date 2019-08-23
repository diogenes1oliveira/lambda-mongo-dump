#!/usr/bin/env python3

'''
Handles S3 multipart uploads from various sources.
'''

import argparse
import logging
import os
import shlex
import subprocess

import boto3

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(os.getenv('LOG_LEVEL', 'INFO'))


class S3MultipartUpload:
    '''
    Represents a multipart upload to S3.
    '''

    # AWS throws EntityTooSmall error for parts smaller than 5 MB
    PART_MINIMUM = 6_000_000

    def __init__(
        self,
        bucket,
        key,
        chunk_size=None,
        buffer_size=None,
        s3=None,
    ):
        '''
        Args:

        - bucket: name of the S3 bucket
        - key: key of the object inside the bucket
        - chunk_size: size in bytes of each part of the upload (default: 15 MB)
        - buffer_size: size in bytes of the output buffer (default: 50 MB)
        '''
        self.bucket = bucket
        self.key = key
        self.chunk_size = chunk_size or 15_000_000
        self.buffer_size = buffer_size or 50_000_000

        assert self.chunk_size >= self.PART_MINIMUM

        self.s3 = s3 or boto3.client("s3")

    def abort_all(self):
        '''
        Aborts all previous multipart uploads for this key.

        Returns a list with the aborted uploads.
        '''
        LOGGER.debug(
            f'Looking for multipart uploads for s3://{self.bucket}/{self.key}')

        mpu_ids = [
            mpu['UploadId']
            for mpu in self.s3.list_multipart_uploads(
                Bucket=self.bucket).get('Uploads', [])
            if mpu['Key'] == self.key
        ]
        abortions = []

        if mpu_ids:
            LOGGER.debug(
                f'Aborting {len(mpu_ids)} multipart uploads: %s', mpu_ids)
            for mpu_id in mpu_ids:
                try:
                    self.s3.abort_multipart_upload(
                        Bucket=self.bucket,
                        Key=self.key,
                        UploadId=mpu_id,
                    )
                except (KeyError, self.s3.exceptions.NoSuchUpload):
                    pass
                else:
                    abortions.append(mpu_id)

        return abortions

    def create(self):
        '''
        Creates the multipart upload, returning its ID.

        This method needs to be called before upload().
        '''
        mpu = self.s3.create_multipart_upload(Bucket=self.bucket, Key=self.key)
        mpu_id = mpu["UploadId"]
        return mpu_id

    def upload_from_stdout(self, mpu_id, cmd_args, capture_stderr=False):
        '''
        Runs the command and sends its output to S3.

        Args:
        - mpu_id: ID of multipart upload, returned by create()
        - cmd_args: command arguments
        - capture_stderr: whether to capture and return the stderr output.

        Returns: (parts, stderr, size)
        - parts: list of the API responses for each uploaded part
        - stderr: captured stderr contents or an empty bytestring
        - size: total uploaded size in bytes
        '''
        parts = []
        uploaded_bytes = 0

        kwargs = {
            'errors': None,
            'stdout': subprocess.PIPE,
            'stderr': subprocess.PIPE if capture_stderr else None,
            'text': None,
            'encoding': None,
            'universal_newlines': None,
            'bufsize': self.buffer_size,
        }

        process = subprocess.Popen(cmd_args, **kwargs)
        i = 1

        for chunk in iter(lambda: process.stdout.read(self.chunk_size), b''):
            LOGGER.debug(
                f'[{self.key}] sizeof PartNumber[%s] = %s', i, len(chunk))
            part = self.s3.upload_part(
                Body=chunk,
                Bucket=self.bucket,
                Key=self.key,
                UploadId=mpu_id,
                PartNumber=i,
            )
            parts.append({
                "PartNumber": i,
                "ETag": part["ETag"],
            })
            uploaded_bytes += len(chunk)
            LOGGER.debug(f'[{self.key}] Uploaded {uploaded_bytes:,} bytes')
            i += 1

        LOGGER.info(f'[{self.key}] command finished sending data')

        try:
            if capture_stderr:
                _, stderr = process.communicate(timeout=1.0)
            else:
                stderr = b''
                process.wait(timeout=1.0)
        except subprocess.TimeoutExpired:
            process.terminate()
            raise

        if process.returncode != 0:
            raise subprocess.CalledProcessError(
                process.returncode,
                ' '.join(shlex.quote(arg) for arg in cmd_args)
            )

        return parts, stderr, uploaded_bytes

    def complete(self, mpu_id, parts):
        '''
        Assembles the uploaded parts and completes the multipart upload.

        Args:
        - mpu_id: ID of multipart upload, returned by create()
        - parts: list of API responses for each uploaded part, as in upload()
        '''
        result = self.s3.complete_multipart_upload(
            Bucket=self.bucket,
            Key=self.key,
            UploadId=mpu_id,
            MultipartUpload={"Parts": parts},
        )
        return result


def parse_args():
    parser = argparse.ArgumentParser(description='Multipart upload')
    parser.add_argument('--bucket', required=True)
    parser.add_argument('--key', required=True)
    parser.add_argument('rest', nargs=argparse.REMAINDER)
    return parser.parse_args()


def main():
    args = parse_args()
    mpu = S3MultipartUpload(
        bucket=args.bucket,
        key=args.key,
    )

    # abort all multipart uploads for this bucket and key
    mpu.abort_all()

    # create new multipart upload
    mpu_id = mpu.create()

    # run the command and upload the parts
    parts, _, size = mpu.upload_from_stdout(mpu_id, cmd_args=args.rest)

    # complete multipart upload
    print(mpu.complete(mpu_id, parts))


if __name__ == "__main__":
    main()

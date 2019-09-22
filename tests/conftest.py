from contextlib import contextmanager
import uuid

import boto3
import docker
from hypothesis import settings, Verbosity
from moto import mock_s3, mock_ssm
import pytest
import responses


settings.register_profile(
    'default',
    max_examples=10,
    deadline=9000,
    verbosity=Verbosity.verbose,
)


@pytest.fixture(scope='function')
def s3():
    with mock_s3():
        responses.add_passthru('http+docker://')
        yield boto3.client('s3', region_name='us-east-1')


@pytest.fixture(scope='function')
def ssm():
    with mock_ssm():
        yield boto3.client('ssm', region_name='us-east-1')


@pytest.fixture
def temp_bucket(s3):
    bucket_name = f'temp-bucket-{uuid.uuid4()}'
    s3.create_bucket(Bucket=bucket_name)
    return bucket_name


@pytest.fixture(scope='function')
def docker_container():
    @contextmanager
    def inner(image, appdir=None, **kwargs):
        if appdir:
            kwargs.setdefault('volumes', {
                appdir: {
                    'bind': '/app',
                    'mode': 'rw',
                },
            })
            kwargs.setdefault('working_dir', '/app')
        client = docker.from_env()
        container = client.containers.run(
            image,
            detach=True,
            auto_remove=True,
            remove=True,
            **kwargs,
        )
        try:
            yield container
        finally:
            container.kill()

    return inner

from pathlib import Path
import tarfile
import urllib.request

import boto3
from hypothesis import settings, Verbosity
from moto import mock_s3, mock_ssm
import pytest


settings.register_profile(
    'default',
    max_examples=10,
    deadline=9000,
    verbosity=Verbosity.verbose,
)


@pytest.fixture(scope='function')
def s3():
    with mock_s3():
        yield boto3.client('s3', region_name='us-east-1')


@pytest.fixture(scope='function')
def ssm():
    with mock_ssm():
        yield boto3.client('ssm', region_name='us-east-1')

import os
from pathlib import Path
import site

import boto3
from hypothesis import settings, Verbosity
from moto import mock_s3
import pytest


site.addsitedir(Path(__file__).absolute().parent / 'custom-site')

settings.register_profile(
    'default', max_examples=10, deadline=9000, verbosity=Verbosity.verbose)


@pytest.fixture(scope='function')
def aws_credentials():
    """
    Mocked AWS Credentials for moto.
    """
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_REGION'] = 'us-east-1'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'


@pytest.fixture(scope='function')
def s3(aws_credentials):
    with mock_s3():
        yield boto3.client('s3', region_name='us-east-1')

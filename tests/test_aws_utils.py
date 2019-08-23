import os

from lambda_mongo_utils import aws_utils


def test_param_injection(ssm, monkeypatch):
    monkeypatch.setenv('MY_PARAM', '')

    ssm.put_parameter(
        Name='/Prod/MyParam',
        Value='my-secret-value',
        Type='String'
    )
    aws_utils.inject_ssm_params_into_env(MY_PARAM='/Prod/MyParam')
    assert os.getenv('MY_PARAM') == 'my-secret-value'


def test_param_injection_decryption(ssm, monkeypatch):
    monkeypatch.setenv('SECRET_VALUE', '')
    monkeypatch.setenv('PUBLIC_VALUE', '')

    ssm.put_parameter(
        Name='/Prod/MySecretParam',
        Value='my-secret-value',
        Type='SecureString'
    )
    ssm.put_parameter(
        Name='/Prod/MyPublicParam',
        Value='my-public-value',
        Type='String'
    )
    aws_utils.inject_ssm_params_into_env(
        decrypt=False,
        SECRET_VALUE='/Prod/MySecretParam',
        PUBLIC_VALUE='/Prod/MyPublicParam',
    )
    assert os.getenv('SECRET_VALUE') != 'my-secret-value'
    assert os.getenv('PUBLIC_VALUE') == 'my-public-value'

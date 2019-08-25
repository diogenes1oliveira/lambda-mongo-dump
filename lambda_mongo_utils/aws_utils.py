#!/usr/bin/env python3

'''
Miscellaneous utilities to interface with AWS APIs.
'''

import os

import boto3


def inject_ssm_params_into_env(decrypt=True, **param_specs):
    '''
    Injects the value of SSM Parameters as environment variables.

    Args:
        decrypt: whether to decrypt SecureString values.
        **param_specs: dict(ENV_NAME=SSM_PARAM_NAME), where
            SSM_PARAM_NAME: name of the SSM Parameter to be fetched
            ENV_NAME: name of the environment variable to the inject as

    Example:

    Assuming that the value of the SSM Parameter:
        /Prod/SecretValue == 'my-secret-value'
        /Basic/OtherValue == 'not-so-secret'

    Calling this function with the following arguments:
        inject_ssm_params_into_env(
            SECRET_VALUE='/Prod/SecretValue',
            OTHER_VALUE='/Basic/OtherValue',
        )

    ...will inject the values as environment variables:

    print(os.getenv('SECRET_VALUE')) # 'my-secret-value'
    print(os.getenv('OTHER_VALUE')) # 'not-so-secret'
    '''
    ssm = boto3.client('ssm')
    response = ssm.get_parameters(
        Names=list(param_specs.values()),
        WithDecryption=decrypt,
    )
    param_values = {p['Name']: p['Value'] for p in response['Parameters']}

    for env_name, ssm_param_name in param_specs.items():
        os.environ[env_name] = param_values[ssm_param_name]

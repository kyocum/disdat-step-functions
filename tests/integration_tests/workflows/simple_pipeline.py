from stepfunctions.steps import *
from caching_util.caching_util import Caching
from tests.integration_tests.utils import VersionChecker
from tests import config
from typing import Any


class SimpleWorkflow:

    @classmethod
    def get_workflow(cls, rerun: bool = False):
        caching = Caching(caching_lambda_name=config.LAMBDA_WORKER_NAME,
                          s3_bucket_url=config.S3_URL,
                          context_name=config.CONTEXT,
                          verbose=True)

        user_pass_1 = Pass(state_id='User Task 1')
        user_pass_2 = Pass(state_id='User Task 2')
        user_pass_1 = caching.cache_step(user_pass_1, bundle_name='pass_bd', force_rerun=True)
        user_pass_2 = caching.cache_step(user_pass_2, bundle_name='pass_bd', force_rerun=rerun)
        path = Chain([user_pass_1, user_pass_2])
        return path

    @classmethod
    def generate_cases(cls) -> list:
        cases = []

        inputs = {'foo': 'bar'}
        expected = inputs
        case = ['simple_workflow', cls.get_workflow(), inputs, expected, ['pass_bd'], [1]]
        cases.append(case)

        inputs = 'string'
        expected = inputs
        case = ['simple_workflow', cls.get_workflow(), inputs, expected, ['pass_bd'], [1]]
        cases.append(case)

        inputs = 123
        expected = inputs
        case = ['simple_workflow', cls.get_workflow(), inputs, expected, ['pass_bd'], [1]]
        cases.append(case)

        inputs = {'dict': {'foo': 'bar'}, 'int': 123, 'bool': True}
        expected = inputs
        case = ['simple_workflow', cls.get_workflow(), inputs, expected, ['pass_bd'], [1]]
        cases.append(case)

        inputs = {'dict': {'foo': 'bar'}, 'int': 123, 'bool': True}
        expected = inputs
        case = ['simple_workflow', cls.get_workflow(True), inputs, expected, ['pass_bd'], [2]]
        cases.append(case)

        return cases

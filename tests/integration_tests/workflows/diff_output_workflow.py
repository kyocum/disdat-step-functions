from stepfunctions.steps import *
from caching_util.caching_util import Caching
from tests.integration_tests.utils import VersionChecker, Config
from typing import Any
import time


class DiffOutWorkflow:

    @classmethod
    def get_workflow(cls, name: str, rerun: bool = False):
        caching = Caching(caching_lambda_name=Config.LAMBDA_WORKER_NAME,
                          s3_bucket_url=Config.S3_URL,
                          context_name=Config.CONTEXT,
                          verbose=True)

        user_pass_1 = Pass(state_id='User Task 1', parameters={'new_field': '123', 'old_data.$': '$'})
        user_pass_1 = caching.cache_step(user_pass_1, bundle_name=name, fore_rerun=rerun)
        path = Chain([user_pass_1])
        return path

    @classmethod
    def generate_cases(cls) -> list:
        cases = []

        inputs = {'foo': 'bar'}
        expected = {'new_field': '123', 'old_data': inputs}
        name = 'diff_pass_bd'
        case = ['diff_out_workflow', cls.get_workflow(name, rerun=True), inputs, expected, [name], [1]]
        cases.append(case)

        inputs = {'foo': 'bar'}
        expected = {'new_field': '123', 'old_data': inputs}
        name = 'diff_pass_bd'
        case = ['diff_out_workflow', cls.get_workflow(name, rerun=False), inputs, expected, [name], [0]]
        cases.append(case)

        inputs = {'foo': 'bar1', 'timestamp': time.time()}
        expected = {'new_field': '123', 'old_data': inputs}
        name = 'diff_pass_bd'
        case = ['diff_out_workflow', cls.get_workflow(name, rerun=False), inputs, expected, [name], [1]]
        cases.append(case)
        return cases
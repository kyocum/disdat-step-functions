from stepfunctions.steps import *
from caching_util.caching_util import Caching
from tests.integration_tests.utils import VersionChecker, Config
from typing import Any
import time


class ParallelWorkflow:

    @classmethod
    def get_workflow(cls, name: str, rerun: bool = False):

        caching = Caching(caching_lambda_name=Config.LAMBDA_WORKER_NAME,
                          s3_bucket_url=Config.S3_URL,
                          context_name=Config.CONTEXT,
                          verbose=True)

        start_state = Pass(state_id='Start Pass')
        pass_1 = Pass(state_id='Pass 1')
        pass_2 = Pass(state_id='Pass 2')

        pass_1 = caching.cache_step(pass_1, bundle_name=name + '_branch1', fore_rerun=rerun)
        pass_2 = caching.cache_step(pass_2, bundle_name=name + '_branch2', fore_rerun=rerun)

        parallel = Parallel(state_id='parallel')

        parallel.add_branch(pass_1)
        parallel.add_branch(pass_2)

        path = Chain([start_state, parallel])
        return path

    @classmethod
    def generate_cases(cls) -> list:
        cases = []

        inputs = 123
        expected = [inputs, inputs]
        name = 'parallel_workflow'
        case = ['parallel_workflow', cls.get_workflow(name, rerun=True), inputs, expected,
                [name + '_branch1', name + '_branch2'], [1, 1]]
        cases.append(case)

        inputs = 123
        expected = [inputs, inputs]
        name = 'parallel_workflow'
        case = ['parallel_workflow', cls.get_workflow(name, rerun=False), inputs, expected,
                [name + '_branch1', name + '_branch2'], [0, 0]]
        cases.append(case)

        inputs = {'timestamp': time.time()}
        expected = [inputs, inputs]
        name = 'parallel_workflow'
        case = ['parallel_workflow', cls.get_workflow(name, rerun=False), inputs, expected,
                [name + '_branch1', name + '_branch2'], [1, 1]]
        cases.append(case)

        return cases

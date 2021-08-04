from stepfunctions.steps import *
from disdat_step_function.caching_wrapper import Caching
from tests.integration_tests.utils import VersionChecker
from tests import config
from typing import Any
import time


class ComplexWorkflow:

    @classmethod
    def get_workflow(cls, rerun: bool = False):

        caching = Caching(caching_lambda_name=config.LAMBDA_WORKER_NAME,
                          s3_bucket_url=config.S3_URL,
                          context_name=config.CONTEXT,
                          verbose=True)

        choice = Choice(state_id='choice_1')
        default_branch = Pass(state_id='pass_1', input_path='$.data_2')

        execution_branch = Pass(state_id='execution', input_path='$.data_1')
        execution_branch = caching.cache_step(execution_branch, force_rerun=rerun, bundle_name='cw_bd_1')

        choice.add_choice(rule=ChoiceRule.BooleanEquals('$.exec', value=True),
                          next_step=execution_branch)
        choice.add_choice(rule=ChoiceRule.BooleanEquals('$.exec', value=False),
                          next_step=default_branch)

        parallel = Parallel(state_id='parallel_1')
        parallel_branch_1 = Pass(state_id='parallel_branch_1', input_path='$[0]')
        parallel_branch_1 = caching.cache_step(parallel_branch_1, force_rerun=rerun, bundle_name='cw_bd_2')
        parallel_branch_2 = Pass(state_id='parallel_branch_2', input_path='$[1]')
        parallel_branch_2 = caching.cache_step(parallel_branch_2, force_rerun=rerun, bundle_name='cw_bd_3')

        parallel.add_branch(parallel_branch_1)
        parallel.add_branch(parallel_branch_2)

        intermediate = Pass(state_id='intermediate')

        default_branch.next(intermediate)
        execution_branch.steps[-1].next(intermediate)
        intermediate.next(parallel)

        final_pass = Pass(state_id='final_pass')
        final_pass = caching.cache_step(final_pass, force_rerun=rerun, bundle_name='cw_bd_4')

        parallel.next(final_pass)
        return choice

    @classmethod
    def generate_cases(cls) -> list:
        cases = []
        name = 'complex_workflow'

        inputs = {'data_1': [1, 2], 'data_2': [3, 4], 'exec': True}
        expected = [1, 2]

        case = [name, cls.get_workflow(rerun=True), inputs, expected,
                ['cw_bd_1', 'cw_bd_2', 'cw_bd_3', 'cw_bd_4'], [1, 1, 1, 1]]
        cases.append(case)

        inputs = {'data_1': [1, 2], 'data_2': [3, 4], 'exec': True, 'irrelevant': [1, 2, 3]}
        expected = [1, 2]

        case = [name, cls.get_workflow(rerun=False), inputs, expected,
                ['cw_bd_1', 'cw_bd_2', 'cw_bd_3', 'cw_bd_4'], [0, 0, 0, 0]]
        cases.append(case)

        inputs = {'data_1': [time.time(), time.time()],
                  'data_2': [3, 4], 'exec': True, 'irrelevant': [1, 2, 3]}
        expected = inputs['data_1']
        #
        case = [name, cls.get_workflow(rerun=False), inputs, expected,
                ['cw_bd_1', 'cw_bd_2', 'cw_bd_3', 'cw_bd_4'], [1, 1, 1, 1]]
        cases.append(case)

        return cases

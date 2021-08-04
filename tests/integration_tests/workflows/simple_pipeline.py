from stepfunctions.steps import *
from disdat_step_function.caching_wrapper import Caching
from tests import config


class SimpleWorkflow:

    @classmethod
    def get_workflow(cls, rerun: bool = False):

        caching = Caching(caching_lambda_name=config.LAMBDA_WORKER_NAME,
                          s3_bucket_url=config.S3_URL,
                          context_name=config.CONTEXT,
                          verbose=True)

        user_pass_1 = Pass(state_id='User Task 1')
        user_pass_1 = caching.cache_step(user_pass_1, bundle_name='user_pass_1', force_rerun=rerun)

        return Chain([user_pass_1])
        # return user_pass_1

    @classmethod
    def generate_cases(cls) -> list:
        cases = []

        inputs = {'foo': '1'}
        expected = inputs
        case = ['simple_workflow', cls.get_workflow(rerun=True), inputs, expected, ['user_pass_1'], [1]]
        cases.append(case)

        # inputs = {'dict': {'foo': 'bar'}, 'int': 123, 'bool': True}
        # expected = inputs
        # case = ['simple_workflow', cls.get_workflow(True), inputs, expected, ['pass_bd'], [1]]
        # cases.append(case)
        #
        inputs = {'foo': '2'}
        expected = inputs
        case = ['simple_workflow', cls.get_workflow(rerun=False), inputs, expected, ['user_pass_1'], [0]]
        cases.append(case)
        #
        # inputs = {'dict': {'foo': 'bar'}, 'int': 123, 'bool': True}
        # expected = inputs
        # case = ['simple_workflow', cls.get_workflow(), inputs, expected, ['pass_bd'], [0]]
        # cases.append(case)

        return cases

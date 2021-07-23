from stepfunctions.steps import states
from caching_util.caching_util import Caching


class SimpleWorkflow:

    @classmethod
    def get_workflow(cls):
        start = states.Pass(state_id='start')
        task_1 = states.Task(state_id='task_1')
        task_2 = states.Task(state_id='task_2')
        exec = states.Chain([task_1, task_2])

        return states.Chain([start, exec])

    @classmethod
    def get_expected_def(cls):
        caching = Caching(caching_lambda_name='',
                          s3_bucket_url='s3://...',
                          context_name='',
                          verbose=True)

        start = states.Pass(state_id='start')
        task_1 = states.Task(state_id='task_1')
        task_2 = states.Task(state_id='task_2')

        task_1 = caching.cache_step(task_1)
        task_2 = caching.cache_step(task_2)

        exec = states.Chain([task_1, task_2])

        return states.Chain([start, exec])

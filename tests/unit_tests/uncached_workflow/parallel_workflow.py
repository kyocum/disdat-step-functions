from stepfunctions.steps import states
from disdat_step_function.caching_wrapper import Caching


class ParallelWorkflow:

    @classmethod
    def get_workflow(cls):
        start = states.Pass(state_id='start')

        task_1 = states.Task(state_id='task_1')
        task_2 = states.Task(state_id='task_2')
        task_3 = states.Task(state_id='task_3')

        parallel = states.Parallel(state_id='parallel')
        parallel.add_branch(task_1)
        parallel.add_branch(task_2)

        end = states.Pass(state_id='end')

        return states.Chain([start, parallel, task_3, end])

    @classmethod
    def get_expected_def(cls):
        caching = Caching(caching_lambda_name='',
                          s3_bucket_url='s3://...',
                          context_name='',
                          verbose=True)

        start = states.Pass(state_id='start')

        task_1 = states.Task(state_id='task_1')
        task_2 = states.Task(state_id='task_2')
        task_3 = states.Task(state_id='task_3')

        task_1 = caching.cache_step(task_1)
        task_2 = caching.cache_step(task_2)
        task_3 = caching.cache_step(task_3)

        parallel = states.Parallel(state_id='parallel')
        parallel.add_branch(task_1)
        parallel.add_branch(task_2)

        end = states.Pass(state_id='end')

        return states.Chain([start, parallel, task_3, end])
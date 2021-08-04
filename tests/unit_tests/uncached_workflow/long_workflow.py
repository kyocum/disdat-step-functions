from stepfunctions.steps import states
from disdat_step_function.caching_wrapper import Caching


class LongWorkflow:

    @classmethod
    def get_workflow(cls):
        start = states.Pass(state_id='start')
        long_path = []
        for i in range(50):
            task = states.Task(state_id='task_' + str(i))
            long_path.append(task)
        exec = states.Chain(long_path)
        return states.Chain([start, exec])

    @classmethod
    def get_expected_def(cls):
        caching = Caching(caching_lambda_name='',
                          s3_bucket_url='s3://...',
                          context_name='',
                          verbose=True)

        start = states.Pass(state_id='start')
        long_path = []
        for i in range(50):
            task = states.Task(state_id='task_' + str(i))
            task = caching.cache_step(task)
            long_path.append(task)
        exec = states.Chain(long_path)
        return states.Chain([start, exec])

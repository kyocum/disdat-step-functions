from stepfunctions.steps import states
from disdat_step_function.caching_wrapper import Caching
from tests import config


class LongWorkflow:
    NUM = 3

    @classmethod
    def get_workflow(cls, name, rerun=False):
        caching = Caching(caching_lambda_name=config.LAMBDA_WORKER_NAME,
                          s3_bucket_url=config.S3_URL,
                          context_name=config.CONTEXT,
                          verbose=True,
                          state_machine_name=name)

        start = states.Pass(state_id='start')
        long_path = []
        for i in range(cls.NUM):
            task = states.Pass(state_id='task_' + str(i))
            task = caching.cache_step(task, force_rerun=rerun)
            long_path.append(task)
        exec = states.Chain(long_path)
        return states.Chain([start, exec])

    @classmethod
    def generate_cases(cls) -> list:
        cases = []

        inputs = {'choice': True}
        expected = inputs
        name = 'long_workflow'
        case = [name, cls.get_workflow(name, rerun=True),
                inputs,
                expected,
                ['task_' + str(i) for i in range(cls.NUM)],
                [1] * cls.NUM,
                ]
        cases.append(case)

        return cases

    # @classmethod
    # def lineage(cls):
    #     dep = {'task_{}'.format(i): 'task_{}'.format(i - 1) for i in range(1, cls.NUM)}
    #     dep['task_0'] = None
    #     return dep
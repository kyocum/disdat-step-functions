from stepfunctions.steps import states, ChoiceRule
from disdat_step_function.caching_wrapper import Caching


class ParallelChoiceMapWorkflow:

    @classmethod
    def get_workflow(cls):
        parallel = states.Parallel('parallel')
        mapper = states.Map('map')
        choice = states.Choice('choice')
        task_1 = states.Task('task_1')
        task_2 = states.Task('task_2')
        task_3 = states.Task('task_3')

        parallel.add_branch(choice)
        choice.add_choice(rule=ChoiceRule.BooleanEquals(variable='$', value=True), next_step=task_1)
        choice.default_choice(next_step=mapper)
        mapper.attach_iterator(task_2)

        parallel.add_branch(states.Pass('pass'))
        return states.Chain([parallel, task_3])

    @classmethod
    def get_expected_def(cls):
        caching = Caching(caching_lambda_name='',
                          s3_bucket_url='s3://...',
                          context_name='',
                          verbose=True)

        parallel = states.Parallel('parallel')
        mapper = states.Map('map')
        choice = states.Choice('choice')
        task_1 = states.Task('task_1')
        task_2 = states.Task('task_2')
        task_3 = states.Task('task_3')

        task_1 = caching.cache_step(task_1)
        task_2 = caching.cache_step(task_2)
        task_3 = caching.cache_step(task_3)

        parallel.add_branch(choice)
        choice.add_choice(rule=ChoiceRule.BooleanEquals(variable='$', value=True), next_step=task_1)
        choice.default_choice(next_step=mapper)
        mapper.attach_iterator(task_2)

        parallel.add_branch(states.Pass('pass'))
        return states.Chain([parallel, task_3])

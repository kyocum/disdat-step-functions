from stepfunctions.steps import states
from stepfunctions.steps import ChoiceRule
from caching_util.caching_util import Caching


class ConditionalChainedWorkflow:

    @classmethod
    def get_workflow(cls):
        start = states.Pass(state_id='start')
        choice = states.Choice(state_id='choice')

        wait_state = states.Wait(state_id='wait', seconds=1)
        task_1 = states.Task(state_id='task_1')
        task_2 = states.Task(state_id='task_2')
        chain = states.Chain([task_1, task_2])
        chain_2 = states.Chain([wait_state, chain])

        choice.add_choice(rule=ChoiceRule.BooleanEquals('$', True), next_step=chain_2)

        task_3 = states.Task(state_id='task_3')
        pass_1 = states.Pass(state_id='pass_1')

        choice.default_choice(next_step=states.Chain([task_3, pass_1]))

        end = states.Pass(state_id='end')
        return states.Chain([start, choice, end])

    @classmethod
    def get_expected_def(cls):
        caching = Caching(caching_lambda_name='',
                          s3_bucket_url='s3://...',
                          context_name='',
                          verbose=True)

        start = states.Pass(state_id='start')
        choice = states.Choice(state_id='choice')

        wait_state = states.Wait(state_id='wait', seconds=1)
        task_1 = states.Task(state_id='task_1')
        task_2 = states.Task(state_id='task_2')

        task_1 = caching.cache_step(task_1)
        task_2 = caching.cache_step(task_2)

        chain = states.Chain([task_1, task_2])
        chain_2 = states.Chain([wait_state, chain])

        choice.add_choice(rule=ChoiceRule.BooleanEquals('$', True), next_step=chain_2)

        task_3 = states.Task(state_id='task_3')
        task_3 = caching.cache_step(task_3)
        pass_1 = states.Pass(state_id='pass_1')

        choice.default_choice(next_step=states.Chain([task_3, pass_1]))

        end = states.Pass(state_id='end')

        return states.Chain([start, choice, end])
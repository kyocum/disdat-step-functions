from disdat_step_function.caching_wrapper import LambdaGenerator
from tests.integration_tests.workflows.complex_workflow import ComplexWorkflow
from stepfunctions.workflow import Workflow
from tests import config
from disdat_step_function.caching_wrapper import Caching
from stepfunctions.steps import *

if __name__ == '__main__':
    # LambdaGenerator.generate(force_rerun=True)'
    # workflow_name = 'complex_workflow'
    #
    # caching = Caching(caching_lambda_name=config.LAMBDA_WORKER_NAME,
    #                   s3_bucket_url=config.S3_URL,
    #                   context_name=config.CONTEXT,
    #                   verbose=True,
    #                   state_machine_name=workflow_name)
    #
    # definition = ComplexWorkflow.get_workflow()
    #
    # target_flow = [flow for flow in Workflow.list_workflows() if flow['name'] == workflow_name]
    # if len(target_flow) > 0:
    #     workflow = Workflow.attach(target_flow[0]['stateMachineArn'])
    #     workflow.update(definition=definition, role=config.EXECUTION_ROLE)
    # else:
    #     workflow = Workflow(workflow_name, definition=definition, role=config.EXECUTION_ROLE)
    #     workflow.create()
    # execution = workflow.execute(inputs={'data': [1,2,3], 'exec': True})
    # result = execution.get_output(wait=True)
    # print(result)

    # print(workflow.definition.to_json(pretty=True))
    from disdat_step_function.caching_wrapper import LambdaGenerator
    LambdaGenerator.generate(root='lambda', force_rerun=True)
import pytest
import os
from stepfunctions.workflow import Workflow
from tests import config
from tests.integration_tests.utils import VersionChecker, LineageChecker
from tests.integration_tests.workflows import simple_pipeline,\
    diff_output_workflow, \
    condition_workflow, \
    parallel_states_workflow, \
    diff_cache_param_workflow, \
    long_workflow, \
    complex_workflow

os.environ['AWS_DEFAULT_REGION'] = config.REGION


test_data = []
# test_data += simple_pipeline.SimpleWorkflow.generate_cases()
# test_data += diff_output_workflow.DiffOutWorkflow.generate_cases()
# test_data += condition_workflow.ConditionWorkflow.generate_cases()
# test_data += parallel_states_workflow.ParallelWorkflow.generate_cases()
# test_data += diff_cache_param_workflow.DiffCacheParamWorkflow.generate_cases()
# test_data += long_workflow.LongWorkflow.generate_cases()
test_data += complex_workflow.ComplexWorkflow.generate_cases()


@pytest.mark.parametrize('workflow_name, definition, inputs, exp_output, bundle_names, gaps', test_data)
def test_workflow(workflow_name, definition, inputs, exp_output, bundle_names, gaps):
    target_flow = [flow for flow in Workflow.list_workflows() if flow['name'] == workflow_name]
    if len(target_flow) > 0:
        workflow = Workflow.attach(target_flow[0]['stateMachineArn'])
        workflow.update(definition=definition, role=config.EXECUTION_ROLE)
    else:
        workflow = Workflow(workflow_name, definition=definition, role=config.EXECUTION_ROLE)
        workflow.create()

    # initialize version checker before the execution to record bundle versions
    checker = VersionChecker(context=config.CONTEXT, bundle_names=bundle_names)
    execution = workflow.execute(inputs=inputs)
    result = execution.get_output(wait=True)

    assert result == exp_output, 'Returned result {} doesn\'t match expected {}'.format(result, exp_output)
    # check if new bundle exists
    for bd, gap in zip(bundle_names, gaps):
        checker.validate_execution(bd=bd, expected_version_gap=gap)
    # LineageChecker.check_lineage(config.CONTEXT, long_workflow.LongWorkflow.lineage())

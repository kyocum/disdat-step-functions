import pytest
from tests.unit_tests.uncached_workflow.simple_workflow import SimpleWorkflow
from tests.unit_tests.uncached_workflow.conditional_workflow import ConditionalWorkflow
from tests.unit_tests.uncached_workflow.conditional_chained_workflow import ConditionalChainedWorkflow
from tests.unit_tests.uncached_workflow.parallel_workflow import ParallelWorkflow
from tests.unit_tests.uncached_workflow.complex_workflow import ComplexWorkflow
from tests.unit_tests.uncached_workflow.long_workflow import LongWorkflow
from tests.unit_tests.uncached_workflow.map_workflow import MapWorkflow
from tests.unit_tests.uncached_workflow.parallel_choice_map_workflow import ParallelChoiceMapWorkflow

from disdat_step_function.caching_wrapper import Caching, PipelineCaching,ExtensiveGraphVisitor


test_data = [
    [SimpleWorkflow.get_workflow(), SimpleWorkflow.get_expected_def()],
    [ConditionalWorkflow.get_workflow(), ConditionalWorkflow.get_expected_def()],
    [ConditionalChainedWorkflow.get_workflow(), ConditionalChainedWorkflow.get_expected_def()],
    [ParallelWorkflow.get_workflow(), ParallelWorkflow.get_expected_def()],
    [ComplexWorkflow.get_workflow(), ComplexWorkflow.get_expected_def()],
    [LongWorkflow.get_workflow(), LongWorkflow.get_expected_def()],
    [MapWorkflow.get_workflow(), MapWorkflow.get_expected_def()],
    [ParallelChoiceMapWorkflow.get_workflow(), ParallelChoiceMapWorkflow.get_expected_def()]
]

caching = Caching(caching_lambda_name='',
                  s3_bucket_url='s3://...',
                  context_name='',
                  verbose=True)


@pytest.mark.parametrize('raw_graph, expected', test_data)
def test_pipeline_caching_graph(raw_graph, expected):
    PipelineCaching(raw_graph, caching).cache()
    visitor_test = ExtensiveGraphVisitor()
    visitor_expected = ExtensiveGraphVisitor()
    # traverse throughout the two pipelines, one generated by PipelineCaching,
    # the other is step-by-step cached by developer
    raw_graph.accept(visitor_test)
    expected.accept(visitor_expected)
    assert len(visitor_test.states) == len(visitor_expected.states)
    for state_id in visitor_test.states:
        assert state_id in visitor_expected.states
        for key in ['Next', 'Type', 'End']:
            if key not in visitor_test.states[state_id] and key not in visitor_expected.states[state_id]:
                continue
            assert visitor_test.states[state_id][key] == visitor_expected.states[state_id][key]



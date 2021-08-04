import pytest
from typing import Iterable
from disdat_step_function.cache_lambda import Cache
import json


def log_loader(filepath: str, page_size: int = 100) -> Iterable:
    with open(filepath, 'r') as fp:
        data = json.load(fp)['events'][::-1]
        # print(data)
        page = []
        for counter, event in enumerate(data):
            page.append(event)
            if counter % page_size == 0:
                yield page
                page = []
        yield page


test_data = [
    # ['./tests/unit_tests/workflow_logs/simple_workflow.txt',
    #  [('task_0', None), ('task_1', 'task_0'), ('task_2', 'task_1')]],
    #
    # ['./tests/unit_tests/workflow_logs/parallel_workflow.txt',
    #  [('pass_1', None), ('pass_2', None)]],

    # ['./tests/unit_tests/workflow_logs/complex_workflow.txt',
    #  [('cw_bd_4', None)]],
# ('cw_bd_1', None), ('cw_bd_2', 'cw_bd_1'), ('cw_bd_3', 'cw_bd_1'),
]


@pytest.mark.parametrize('file_path, dependency', test_data)
def test_lineage_tracer(file_path: str, dependency: list):
    for (child_name, parent_name) in dependency:
        logs = log_loader(file_path, page_size=100)
        event = Cache.lineage_tracer(logs, bundle_name=child_name)
        # print(child_name, event)
        if event is None:
            assert parent_name is None, 'lineage tracer failed to identify the parent for {}'.format(child_name)
        else:
            assert event.bundle_name == parent_name, 'lineage tracer identify a wrong parent for {}'.format(child_name)


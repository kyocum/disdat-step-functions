import pytest
from caching_util.cache_lambda import Cache, PathParam as pp
from disdat import api
from typing import Any, Union
import json
import inspect
from tests import config


S3_URL = config.TEST_S3_BUCKET
CONTEXT = 'test_cache_pull'

api.context(CONTEXT)
api.remote(CONTEXT, remote_context=CONTEXT, remote_url=S3_URL)


def generate_input_event(full_params: Any,
                         bundle_name: str,
                         force_run: bool = False,
                         cache_params: Union[dict, str] = None) -> dict:

    if cache_params is None:
        cache_params = full_params
    dsdt_args = {'s3_bucket_url': S3_URL,
                 'context': CONTEXT,
                 'force_rerun': force_run,
                 'verbose': True,
                 'bundle_name': bundle_name,
                 'delocalized': True
                 }
    data = {pp.FULL_PARAM: full_params, pp.CACHE_PARAM: cache_params, pp.DSDT_ONLY_ARGS: dsdt_args}
    return data


def push_data_to_s3(params: Any, data: Any, bundle_name: str):
    signature = {'input_params': json.dumps(params)}
    proc_name = api.Bundle.calc_default_processing_name(bundle_name, signature, dep_proc_ids={})
    with api.Bundle(CONTEXT, name=bundle_name, processing_name=proc_name) as b:
        file = b.get_file('cached_data.json')
        with open(file, 'w') as f:
            json.dump(data, f)
        b.add_params(signature)
        b.add_data(file)
    api.commit(CONTEXT, bundle_name)
    api.push(CONTEXT, bundle_name=bundle_name, delocalize=True)


def check_integrity(saved: Any, expected_data: Any, expected_param: Any):
    assert saved[pp.CACHE_DATA] == expected_data
    assert saved[pp.CACHE_PARAM] == expected_param


test_data = [
    ('dict_input', {'foo': 'bar'}, None, False, True),
    ('int_input', 123, None, False, True),
    ('list_input', ['J', 'Q', 'K'], None, False, True),
    ('force_run_input', {'foo': 'bar'}, None, True, False),
    ('diff_param_input', {'foo': 'bar'}, 'bar', False, True),
    ('complex_input', {'dict': {'list': [1, 2, 3]}, 'int': 123, 'str': 'foobar'}, {'list': [1, 2, 3]}, False, True),
]


"""
Test caching pull 
"""

@pytest.mark.parametrize('bd_name,data,params,force_rerun,should_use_cache', test_data)
def test_cache_pull(bd_name, data, params, force_rerun, should_use_cache):
    func_name = inspect.currentframe().f_code.co_name
    # generate some input data
    event = generate_input_event(full_params=data, cache_params=params, bundle_name=func_name, force_run=force_rerun)
    # push mock data to S3 and remove local bundle
    push_data_to_s3(params=event[pp.CACHE_PARAM], data=data, bundle_name=func_name)
    api.rm(CONTEXT, bundle_name=bd_name, rm_all=True)
    bundle = api.search(CONTEXT, human_name=bd_name)
    assert len(bundle) == 0, 'local bundles not cleared!'

    # pull data from S3 and check integrity
    output = Cache(event[pp.DSDT_ONLY_ARGS]).cache_pull(event)
    assert output[pp.USE_CACHE] == should_use_cache
    if should_use_cache:
        check_integrity(output, expected_data=data, expected_param=event[pp.CACHE_PARAM])
    else:
        check_integrity(output, expected_data=None, expected_param=event[pp.CACHE_PARAM])


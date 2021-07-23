import logging
import pytest
from caching_util.cache_lambda import Cache, PathParam as pp
from disdat import api
from typing import Any, Union
import json
import inspect
import os
import logging


HOME = '/Users/zzhang2'
os.environ["HOME"] = HOME

S3_URL = 's3://step-function-cache-bucket'
CONTEXT = 'test_cache_pull'

api.context(CONTEXT)
api.remote(CONTEXT, remote_context=CONTEXT, remote_url=S3_URL)
logging.basicConfig(level=logging.INFO)


def generate_input_event(full_params: Any,
                         bundle_name: str,
                         from_cache_pull: bool,
                         cache_params: Union[dict, str] = None) -> dict:

    if cache_params is None:
        cache_params = full_params
    dsdt_args = {'s3_bucket_url': S3_URL,
                 'context': CONTEXT,
                 'verbose': True,
                 'bundle_name': bundle_name,
                 'delocalized': True
                 }
    if not from_cache_pull:
        data =[{pp.CACHE_PARAM: cache_params}, full_params]
    else:
        data = {pp.FULL_PARAM: None, pp.CACHE_PARAM: None,
                pp.CACHE_DATA: full_params, pp.USE_CACHE: True}
    return {pp.FULL_PARAM: data, pp.DSDT_ONLY_ARGS: dsdt_args}


def pull_data_from_s3(bundle_name: str, cache_params: Any):
    signature = {'input_params': json.dumps(cache_params)}
    proc_name = api.Bundle.calc_default_processing_name(bundle_name, signature, dep_proc_ids={})
    api.pull(CONTEXT, bundle_name)
    bundle = api.search(CONTEXT, processing_name=proc_name)
    if len(bundle) > 0:
        latest_bundle = bundle[0]
        api.pull(CONTEXT, uuid=latest_bundle.uuid, localize=True)
        file = latest_bundle.data
        with open(file, 'r') as fp:
            cached_data = json.load(fp)
        cached_param = latest_bundle.params['input_params']
    else:
        cached_data = None
        cached_param = None
    return cached_data, cached_param


test_data = [
    ('simple_push', {'foo': 'bar'}, None, False),
    ('complex_push', {'dict': {'list': [1, 2, 3]}, 'int': 123, 'str': 'foobar'}, None, False),
    ('int_push', 123, None, False),
    ('string_push', '123', None, False),
    ('diff_param', '123', 'jqk', False),

    ('simple_no_push', {'foo': 'bar'}, None, True),
    ('complex__no_push', {'dict': {'list': [1, 2, 3]}, 'int': 123, 'str': 'foobar'}, None, True),
    ('int_no_push', 123, None, True),
    ('string_no_push', '123', None, True),
    ('diff_no_param', '123', 'jqk', True)
]


@pytest.mark.parametrize('bundle_name,data,params,from_cache_pull', test_data)
def test_cache_pull(bundle_name, data, params, from_cache_pull):
    event = generate_input_event(data, bundle_name=bundle_name, from_cache_pull=from_cache_pull, cache_params=params)
    api.rm(CONTEXT, bundle_name=bundle_name, rm_all=True)
    bundle = api.search(CONTEXT, human_name=bundle_name)
    assert len(bundle) == 0, 'local bundles not cleared!'
    returned = Cache(event[pp.DSDT_ONLY_ARGS]).cache_push(event)
    assert returned == data



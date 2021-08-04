import json
import logging
from typing import Any, Union

import pytest
from disdat import api

from disdat_step_function.cache_lambda import Cache, PathParam as pp
from tests import config

S3_URL = config.TEST_S3_BUCKET
CONTEXT = 'cache_push_testing'

api.context(CONTEXT)
api.remote(CONTEXT, remote_context=CONTEXT, remote_url=S3_URL)
logging.basicConfig(level=logging.INFO)


def generate_input_event(full_params: Any,
                         bundle_name: str,
                         from_cache_pull: bool,
                         cache_params: Union[dict, str] = None) -> dict:

    if cache_params is None:
        cache_params = full_params
    # prepares dsdt args
    dsdt_args = {'s3_bucket_url': S3_URL,
                 'context': CONTEXT,
                 'verbose': True,
                 'bundle_name': bundle_name,
                 'delocalized': True
                 }
    # if from caching pull, then the data is a list
    if not from_cache_pull:
        data =[{pp.CACHE_PARAM: cache_params}, full_params]
    # other wise a dictionary
    else:
        data = {pp.FULL_PARAM: None, pp.CACHE_PARAM: None,
                pp.CACHE_DATA: full_params, pp.USE_CACHE: True}
    return {pp.FULL_PARAM: data, pp.DSDT_ONLY_ARGS: dsdt_args}


def pull_data_from_s3(bundle_name: str, cache_params: Any):
    """
    Given a bundle name and cache parameters, calculate proc name and pull from S3
    This is used to verify the correctness of caching push

    :param bundle_name: str, bundle name
    :param cache_params: Any, bundle parameter signature
    :return:
    """
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
    # (bundle name, data, param to cache(None means cache everything), from caching pull
    #   (False means no push and simply pass the data to the next state ))
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

"""
Test caching push
"""


@pytest.mark.parametrize('bundle_name,data,params,from_cache_pull', test_data)
def test_cache_push(bundle_name, data, params, from_cache_pull):
    event = generate_input_event(data, bundle_name=bundle_name, from_cache_pull=from_cache_pull, cache_params=params)
    api.rm(CONTEXT, bundle_name=bundle_name, rm_all=True)
    bundle = api.search(CONTEXT, human_name=bundle_name)
    assert len(bundle) == 0, 'local bundles not cleared!'

    returned = Cache(event[pp.DSDT_ONLY_ARGS]).cache_push(event)
    assert returned == data

    if not from_cache_pull:
        # if from the execution branch, then push is evoked! pull data from remote and check integrity
        cached_data, cached_params = pull_data_from_s3(bundle_name, event[pp.FULL_PARAM][0][pp.CACHE_PARAM])
        assert cached_data == data


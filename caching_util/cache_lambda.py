import json
from disdat import api
import logging
from typing import Any
from stepfunctions.workflow import Workflow


LOG_LEVEL = logging.INFO + 1


class PathParam:
    FULL_PARAM = '_full_params'
    CACHE_PARAM = '_cache_params'
    DSDT_PASS_PARAM = '_dsdt_params'
    DSDT_ONLY_ARGS = '_dsdt_only_args'
    USE_CACHE = '_use_cache'
    CACHE_DATA = '_data'

    DSDT_PASS_PARAM_SUFFIX = '{}.$'.format(DSDT_PASS_PARAM)
    DSDT_ONLY_ARGS_SUFFIX = '{}.$'.format(DSDT_ONLY_ARGS)

    FULL_PARAM_PREFIX = '$.{}'.format(FULL_PARAM)
    FULL_PARAM_SUFFIX = '{}.$'.format(FULL_PARAM)

    CACHE_PARAM_PREFIX = '$.{}'.format(CACHE_PARAM)
    CACHE_PARAM_SUFFIX = '{}.$'.format(CACHE_PARAM)

    USE_CACHE_PREFIX = '$.{}'.format(USE_CACHE)


class Cache:

    def __init__(self, dsdt_args):
        """
        Class that implements cache-pull and cache-push
        :param dsdt_args: dict, parameters necessary for disdat to set up and talk to remote contexts
            the params are supplied via the definition json (users don't need to do this as Caching.cache_step handles
            this)
        """
        self.context = dsdt_args['context']
        self.s3_url = dsdt_args['s3_bucket_url']
        self.bundle_name = dsdt_args['bundle_name']
        self.force_rerun = dsdt_args.get('force_rerun', False)
        self.verbose = dsdt_args.get('verbose', False)
        self.delocalize = dsdt_args.get('delocalized', False)

        if self.verbose:
            level = LOG_LEVEL
        else:
            level = LOG_LEVEL + 1
        if len(logging.getLogger().handlers) > 0:
            logging.getLogger().setLevel(level=level)
        else:
            logging.basicConfig(format='%(asctime)s %(message)s', level=level)
        # set up local context
        api.context(self.context)
        # set up and bind with the remote context
        api.remote(self.context, remote_context=self.context, remote_url=self.s3_url)

    def cache_pull(self, event: Any) -> dict:
        """
        pulls data from the remote context specified by the constructor. I
        :param event: event data from the lambda function
        :return: dict, {PathParam.FULL_PARAM: Any, full data from the successor ,
                        PathParam.CACHE_PARAM: Any, what parameters to cache ,
                        PathParam.CACHE_DATA: Any, not null if a cache hit,
                        PathParam.USE_CACHE: bool, flag for the choice state }
        """
        full_params = event[PathParam.FULL_PARAM]
        cache_params = event[PathParam.CACHE_PARAM]
        use_cache, cached_data = False, None
        # since all states share the same lambda, we need a better identifier for the logs
        func_name = 'cache_pull_4_'.format(self.bundle_name)
        logging.log(level=LOG_LEVEL, msg='{} - received input event {}'.format(func_name, event))

        if not self.force_rerun:
            signature = {'input_params': json.dumps(cache_params)}
            # uniquely determine a proc name based on bundle name and signature
            proc_name = api.Bundle.calc_default_processing_name(self.bundle_name, signature, dep_proc_ids={})
            # pull bundle meta data from s3
            api.pull(self.context, self.bundle_name)
            # search if the proc name exists
            bundle = api.search(self.context, processing_name=proc_name)
            # could have multiple hits because of forced reruns
            if len(bundle) > 0:
                # use the latest cache data
                latest_bundle = bundle[0]
                # check if the signature match
                use_cache = True not in [v != latest_bundle.params.get(k, None)
                                         for k, v in signature.items()]
                # if use cache is true, pulls the actual data (the json file that holds the cached data) from s3
                if use_cache:
                    api.pull(self.context, uuid=latest_bundle.uuid, localize=True)
                    file = latest_bundle.data
                    with open(file, 'r') as fp:
                        cached_data = json.load(fp)
        # return the result, full param is what the user step expects to receive, so we need to forward it
        # cache_params is needed by cache push to create bundles
        # cached_data is needed by cache push if use_cache is true
        # use_cache is needed by the choice state
        data = {PathParam.FULL_PARAM: full_params, PathParam.CACHE_PARAM: cache_params,
                PathParam.CACHE_DATA: cached_data, PathParam.USE_CACHE: use_cache}
        logging.log(level=LOG_LEVEL, msg='{} - outputs - {}'.format(func_name, data))
        return data

    def cache_push(self, event: Any) -> Any:
        """
        pushes data to the remote context is use cache is false
        otherwise simply parse the input event and return the cached data
        :param event: Any, whatever data received by the lambda function
        :return: Any, data output by the user step
        """
        full_params = event[PathParam.FULL_PARAM]
        func_name = 'cache_push_4_'.format(self.bundle_name)
        logging.log(level=LOG_LEVEL, msg='{} - received input event {}'.format(func_name, event))

        # if the input event has type dict, it must comes from caching pull directly (use cache = true)
        if isinstance(full_params, dict):
            logging.log(level=LOG_LEVEL, msg='{} - no push'.format(func_name))
            # simply grab the cached data and return
            return full_params[PathParam.CACHE_DATA]

        # if the input is a list, then we know it's from the execution branch
        elif isinstance(full_params, list):
            # get the parameters that the user step used to generate output
            if PathParam.CACHE_PARAM in full_params[0]:
                cache_params = full_params[0][PathParam.CACHE_PARAM]
                params_to_save = full_params[1]
            elif PathParam.CACHE_PARAM in full_params[1]:
                cache_params = full_params[1][PathParam.CACHE_PARAM]
                params_to_save = full_params[0]
            else:
                raise ValueError('Key _cache_params is expected but not present')
            # create bundle signature
            signature = {'input_params': json.dumps(cache_params)}
            proc_name = api.Bundle.calc_default_processing_name(self.bundle_name, signature, dep_proc_ids={})
            with api.Bundle(self.context, name=self.bundle_name, processing_name=proc_name) as b:
                # write the output data to a json file. This will speed up caching pull
                # because we only pull the actual data after a signature match
                file = b.get_file('cached_data.json')
                with open(file, 'w') as f:
                    json.dump(params_to_save, f)
                b.add_params(signature)
                b.add_data(file)
            # commit and push the data to the remote context
            api.commit(self.context, self.bundle_name)
            api.push(self.context, bundle_name=self.bundle_name, delocalize=self.delocalize)
            logging.log(level=LOG_LEVEL,
                        msg='{} - data pushed. Cached parameters: {}, cached data: {}'\
                        .format(func_name, cache_params, params_to_save))
            return params_to_save

        else:
            raise TypeError("field {} must have type dict or list; {} is provided".format(
                PathParam.FULL_PARAM, type(full_params)))




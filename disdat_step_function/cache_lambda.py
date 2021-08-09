"""
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
"""


import collections
import json

import boto3
from disdat import api
import logging
from typing import Any, Union
import json


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
        self.state_machine_name = dsdt_args.get('state_machine_name', '')

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
        func_name = 'cache_pull_4_{}'.format(self.bundle_name)
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
                logging.log(level=LOG_LEVEL, msg='{} - {} bundles found with name {}'.format(func_name,
                                                                                              len(bundle),
                                                                                              self.bundle_name))
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

    def cache_push(self, event: Any, parent: Union[None, api.Bundle] = None) -> Any:
        """
        pushes data to the remote context is use cache is false
        otherwise simply parse the input event and return the cached data
        :param event: Any, whatever data received by the lambda function
        :return: Any, data output by the user step
        """

        logging.log(level=LOG_LEVEL, msg='found parent {}'.format(parent.uuid if parent else None))

        full_params = event[PathParam.FULL_PARAM]
        func_name = 'cache_push_4_{}'.format(self.bundle_name)
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
                if parent is not None:
                    b.add_dependencies(parent)

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

    def get_lineage(self):
        logging.log(level=LOG_LEVEL, msg='run lineage tracing for {}'.format(self.state_machine_name))
        logs = self._get_execution_history_by_name(self.state_machine_name)
        parent_exec = self.lineage_tracer(logs, self.bundle_name)
        return self._get_bundle(parent_exec)

    def _get_execution_history_by_name(self, workflow_name):
        client = boto3.client('stepfunctions')
        workflow = []
        try:
            response = client.list_state_machines(maxResults=100)
            workflow += response['stateMachines']
            while 'nextToken' in response:
                response = client.list_state_machines(maxResults=100, nextToken=response['nextToken'])
                workflow += response['stateMachines']

            state_machine = [w for w in workflow if w['name'] == workflow_name]
            sm_arn = state_machine[0]['stateMachineArn']

            curr_execs = client.list_executions(stateMachineArn=sm_arn, statusFilter='RUNNING')['executions']
            assert len(curr_execs) <= 1, 'lineage capture may be incorrect if you have ' \
                                         'current executions, disabling lineage'
            logs = client.get_execution_history(executionArn=curr_execs[0]['executionArn'],
                                                maxResults=1000,
                                                reverseOrder=True)
            start = True
            while 'nextToken' in logs or start:
                if start:
                    logs = client.get_execution_history(executionArn=curr_execs[0]['executionArn'],
                                                        maxResults=1000,
                                                        reverseOrder=True)
                else:
                    logs = client.get_execution_history(executionArn=curr_execs[0]['executionArn'],
                                                        maxResults=1000,
                                                        reverseOrder=True,
                                                        nextToken=logs['nextToken'])
                start = False
                yield logs['events']

        except Exception as e:
            logging.warning('lambda failed to retrieve execution log. Check your lambda permission!')
            logging.warning('Error log ' + str(e))
            yield None

    @classmethod
    def lineage_tracer(cls, logs, bundle_name: str) -> Union[None, 'ExecutionEvent']:
        target_name = 'cache_pull_{}'.format(bundle_name)
        target_node = None
        events_by_id = {}
        children_by_id = collections.defaultdict(set)

        for page in logs:
            if page is None:
                return None
            # build partial DAG
            for event in page:
                event_id, parent_id = event['id'], event['previousEventId']
                children_by_id[parent_id].add(event_id)

                if event_id not in event:
                    events_by_id[event_id] = ExecutionEvent(event)
                event_obj = events_by_id[event_id]
                if event_id in children_by_id:
                    for child_id in children_by_id[event_id]:
                        if events_by_id[child_id].parent is None:
                            events_by_id[child_id].parent = event_obj
                            print(events_by_id[child_id])
            # find the target node
            for node in events_by_id.values():
                if node.name is not None and node.name == target_name:
                    target_node = node
                    break
            # target node not found in this page, go to the next page
            if target_node is None:
                continue

            parent_id = target_node.find_prev_bundle()
            if parent_id is None:
                continue
            else:
                return events_by_id[parent_id]
        # parent bundle not found
        # print([str(event_obj) for event_obj in events_by_id.values()])
        return None

    def _get_bundle(self, execution) -> Union[None, api.Bundle]:
        if execution is None:
            return None
        parent_bundle_name = execution.bundle_name
        output_params = json.loads(execution.full_event['stateExitedEventDetails']['output'])
        cache_params = output_params[PathParam.CACHE_PARAM]
        signature = {'input_params': json.dumps(cache_params)}
        proc_name = api.Bundle.calc_default_processing_name(parent_bundle_name, signature, dep_proc_ids={})
        api.pull(self.context, parent_bundle_name)
        bundles = api.search(self.context, parent_bundle_name, processing_name=proc_name)
        return bundles[0] if len(bundles) > 0 else None


class ExecutionEvent:
    def __init__(self, event: dict):
        self.full_event = event
        self.id = self.full_event['id']
        self.name = None
        self.parent = None
        self.bundle_name = None
        # self.children = None

        if 'stateExitedEventDetails' in self.full_event and 'name' in self.full_event['stateExitedEventDetails']:
            name = self.full_event['stateExitedEventDetails']['name']
            if name.startswith('cache_pull'):
                self.name = name
                self.bundle_name = self.name[len('cache_pull_'):]

    def find_prev_bundle(self) -> Union[None, str]:
        parent = self.parent
        counter = 0
        while parent is not None:

            if parent.bundle_name is not None:
                counter += 1
                if counter > 1:
                    return parent.id
            else:
                parent = parent.parent
        return None

    # def find_next_bundle(self) -> Union[None, set]:
    #
    #     children = self.children

    def __str__(self):
        return '{} -> {}; Name={}; bundle={}'.format(self.id, self.parent.id if self.parent else None,
                                                     self.name, self.bundle_name)
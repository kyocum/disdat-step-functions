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


import pathlib
import time
from typing import Union
from stepfunctions import steps, inputs
from stepfunctions.steps import states
from stepfunctions.steps.fields import Field
import os
import shutil
from disdat_step_function.cache_lambda import PathParam as pp
import logging


class Caching:

    def __init__(self,
                 caching_lambda_name: str,
                 s3_bucket_url: str,
                 context_name: str,
                 state_machine_name: str = '',
                 force_rerun: bool = False,
                 verbose: bool = False):
        """
        This class initializes a caching object that contains basic specs of the caching layer
        :param caching_lambda_name: str, name of the lambda function. For instance 'caching-lambda'
        :param s3_bucket_url: str, link to the s3 bucket, in the format of 's3://BUCKET_NAME'
        :param context_name: str, the name of the context in which data versions are managed
        :param force_rerun: bool, rerun states by force
        :param verbose: bool, true to see detailed logs from the caching code
        """
        self.caching_lambda = caching_lambda_name
        self.s3_bucket = s3_bucket_url
        self.context_name = context_name
        self.force_rerun = force_rerun
        self.state_machine_name = state_machine_name
        self.verbose = verbose
        # kwargs passed to the caching lambda, users don't need to worry about this
        self.disdat_args = {'s3_bucket_url': self.s3_bucket,
                            'context': self.context_name,
                            'force_rerun': self.force_rerun,
                            'verbose': self.verbose,
                            'state_machine_name': self.state_machine_name}
        assert self.s3_bucket.startswith('s3://'), 's3 bucket url invalid format'
        assert isinstance(self.verbose, bool), 'verbose has the wrong type, bool expected'
        assert isinstance(self.force_rerun, bool), 'force_rerun has the wrong type, bool expected'

    def cache_step(self, user_step: steps.states, bundle_name: str = None, force_rerun: bool = False) -> steps.Chain:
        """
        enable caching for the input user step by wrapping the user step in a chain of generated caching states
        for instance:
            input: task A
            output: chain that contains caching pull -> choice ->(rerun)-> run task A -> caching push
                                                               |-> (no rerun) --------|

        :param user_step: steps.states, a stepfunction state object
        :param bundle_name: str, the unified bundle name for all data generated by this user step
        :param force_rerun: bool, override the object-level force rerun setting
        :return: steps.Chain, a mini DAG that implements the caching logic
        """
        task_name = user_step.state_id.replace(' ', '_').lower()
        # bundle name is automatically assigned if not set
        if bundle_name is None:
            bundle_name = task_name
        self.disdat_args['bundle_name'] = bundle_name
        # copy the dict parameters because it is shared by potentially many cache_step calls.
        disdat_args = self.disdat_args.copy()
        disdat_args['force_rerun'] = force_rerun
        disdat_args['time'] = time.time()
        # set the caching pull input path to match user step's input path, we do this to avoid
        # caching unnecessary params that are not consumed by user step
        user_inputs = user_step.fields.get(Field.InputPath.value, '$')
        # define caching pull state
        cache_pull = steps.LambdaStep(state_id='cache_pull_{}'.format(task_name),
                                      output_path='$.Payload',
                                      parameters={
                                          'FunctionName': self.caching_lambda,
                                          'Payload': {pp.FULL_PARAM_SUFFIX: '$',
                                                      pp.CACHE_PARAM_SUFFIX: user_inputs,
                                                      pp.DSDT_ONLY_ARGS: disdat_args
                                                      }
                                        }
                                      )
        # define caching push state
        # note that the two states share the same lambda function!
        # as a matter of fact, this one lambda is called repeated by all cached states
        cache_push = steps.LambdaStep(state_id='cache_push_{}'.format(task_name),
                                      output_path='$.Payload',
                                      parameters={
                                          'FunctionName': self.caching_lambda,
                                          'Payload': {pp.FULL_PARAM_SUFFIX: '$',
                                                      pp.DSDT_ONLY_ARGS: disdat_args
                                                      }
                                        }
                                      )

        cache_condition = steps.Choice(state_id='use_cache?_{}'.format(task_name))
        # define the execution branch (run when input param signature does not match cache)
        execution_branch = steps.Parallel(state_id='execute_{}'.format(task_name))
        # we need a pass state to pass the inputs of the user step to caching push
        # otherwise we cannot recover the inputs based on user step's output
        execution_branch.add_branch(steps.Pass('param_pass_{}'.format(task_name),
                                               input_path=pp.CACHE_PARAM_PREFIX,
                                               parameters={pp.CACHE_PARAM_SUFFIX: '$'}))
        # because the output of caching pull has the following format
        # {'full param': ..., 'cache param': ..., 'use_cache':...}
        # it is important that we pass only the full_param field to the user step as expected by the user
        # The caching layer should be invisible
        param_resolve = steps.Pass('param_resolve_{}'.format(task_name), input_path=pp.FULL_PARAM_PREFIX)
        # param resolver is ran before the user step to properly prepare the input data
        execution_branch.add_branch(steps.Chain([param_resolve, user_step]))
        execution_branch = steps.Chain([execution_branch, cache_push])
        # if 'use_cache' is false, run caching push (it does not actually push, just simply parse and return the data)
        # if 'use_cache' is true, run the execution branch as well as caching push (create new version in ctxt)
        cache_condition.add_choice(rule=steps.ChoiceRule.BooleanEquals(pp.USE_CACHE_PREFIX, value=False),
                                   next_step=execution_branch)

        return steps.Chain([cache_pull, cache_condition, cache_push])


class PipelineCaching:

    def __init__(self, definition: Union[states.Chain, states.State], caching: Caching):
        """
        Cache all tasks in a pre-existing state machine
        :param definition: states.Chain, the state machine that the user wants to cache
        :param caching: Caching, the caching utility object used for caching individual states
        """
        visitor = StateVisitor(caching)
        definition.accept(visitor)

        self.definition = definition
        self.states = visitor.states
        # to replace keeps track of tasks to replace (determined by StateVisitor on the fly)
        self.to_replace = visitor.to_replace
        self.visited = {}

        if len(logging.getLogger().handlers) > 0:
            logging.getLogger().setLevel(level=logging.WARNING)
        else:
            logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.WARNING)

    def cache(self):
        """
        Cache the state machine passed to the constructor
        :return:
        """
        self.visited = {}
        self._overwrite(self.definition)

    def _overwrite(self, state: Union[states.Chain, states.State]) -> Union[states.Chain, states.State, None]:
        """
        dfs graph traversal algorithm. The logic is straightforward and is stated below:
            for a given state/chain,
            return the in-place modified input with necessary tasks replaced with cached wrappers

        :param state: Union[states.Chain, states.State], the state/chain to modify in-place
        :return: modified state
        """

        if state is None:
            return state

        # we should not visit the same node twice as the function is not idempotent
        # if a state is visited, the next visit will inject another wrapper inside the existing wrapper
        if state.state_id in self.visited:
            return self.visited[state.state_id]

        # if the input is a chain, simply iterate through it and modify all elements
        if isinstance(state, states.Chain):
            for idx, step in enumerate(state.steps):
                state.steps[idx] = self._overwrite(step)
            return state

        # if the input is a choice state, we must enter the branches as choice is not supposed to have next_step
        elif isinstance(state, states.Choice):
            for idx, (_, next_s) in enumerate(state.choices):
                # modify the branches
                state.choices[idx][1] = self._overwrite(next_s)
            # the default option is not included in state.choices
            state.default = self._overwrite(state.default)
            # ideally choice state should not have next_step. This line handles the edge case
            # where the user explicitly assigned a next_step for choice state
            # Without this line, the state machine may become disconnected
            state.next_step = self._overwrite(state.next_step)

        elif isinstance(state, states.Parallel):
            # logging.warning('Be careful that states inside any parallel state are ' +
            #                 'not cached because of a stepfunction bug')
            # iterate through the branches
            # transform them before moving forward to next_step
            for idx, next_s in enumerate(state.branches):
                state.branches[idx] = self._overwrite(next_s)
            # modify the subsequent state, otherwise it still points to the old state
            state.next_step = self._overwrite(state.next_step)

        elif isinstance(state, states.Map):
            # logging.warning('Be careful that states inside any map state are ' +
            #                 'not cached because of a stepfunction bug')
            # transform the iterator before moving forward to next_step
            state.iterator = self._overwrite(state.iterator)
            # modify the subsequent state, otherwise it still points to the old state
            state.next_step = self._overwrite(state.next_step)

        else:
            # if state is not marked for replacement, simply skip it
            # note that if a subsequent state is marked for replacement,
            # we need to fix the state.next_step pointer
            if state.state_id not in self.to_replace:
                state.next_step = self._overwrite(state.next_step)

            else:
                # note that replacement is the caching wrapper, hence it is a chain
                replacement = self.to_replace[state.state_id]
                # make sure caching_push_xxx points to the subsequent state of the input state
                replacement.steps[-1].next_step = self._overwrite(state.next_step)
                # state is inside replacement, it should not point to the original next_step
                state.next_step = None
                # mark the node as visited
                self.visited[state.state_id] = replacement
                return replacement

        # if a node is revisited, simply return the modified version
        self.visited[state.state_id] = state
        return state


class LambdaGenerator:
    CACHING_LAMBDA_DIR = 'cache_lambda'
    DEPENDENCY = 'dependency'
    DISDAT_LAYER = 'disdat_caching_layer'
    CACHING_LAMBDA_SR_PY = 'cache_lambda.py'
    LAMBDA_STUB_PY = 'lambda_for_user.py'

    @classmethod
    def generate(cls, root: Union[str, pathlib.Path] = 'generated_lambda', force_rerun: bool = False):
        """
        generate the lambda src code and a layer that users can use to create the lambda function.
        The lambda function will need access to the disdat package, which is provided as a lambda layer
        (pip install not supported by aws lambda)

        Note that installing disdat locally and zip it won't work, as some binaries are not portable. Currently
        we wrote a simple docker pip utility that installs the package in docker container (amazonlinux:2) and
        export it as a zip

        output format
            root
                - /caching_lambda
                - /dependency
                - disdat_caching_layer.zip
        :param root: str, where do you want to save the code
        :param force_rerun: bool, force rerun docker pip install even if the dependency folder is present
        :return: None
        """
        root = os.path.abspath(root)
        os.makedirs(os.path.join(root, cls.CACHING_LAMBDA_DIR), exist_ok=True)
        # copy the lambda src code. TODO: remove this after PR as caching_lambda can be accessible from disdat
        src = os.path.join(os.path.dirname(__file__), cls.CACHING_LAMBDA_SR_PY)
        dst = os.path.join(root, cls.CACHING_LAMBDA_DIR, cls.CACHING_LAMBDA_SR_PY)
        shutil.copyfile(src=src, dst=dst)
        # copy the lambda stub code that users need to create a lambda function on AWS
        src = os.path.join(os.path.dirname(__file__), cls.LAMBDA_STUB_PY)
        dst = os.path.join(root, cls.CACHING_LAMBDA_DIR, cls.LAMBDA_STUB_PY)
        shutil.copyfile(src=src, dst=dst)
        print('lambda code generated! find it in {}'.format(dst))
        # run docker pip install
        cls.install_dep(root, force_rerun)
        print("\nINSTRUCTIONS:\nPlease create a lambda function with from the generated .py file" +
              "and a lambda layer from the generated zip!\n" +
              "IMPORTANT TIPS: \nGive the lambda function proper S3 bucket permissions and timeout of at least 20s")

    @classmethod
    def install_dep(cls, root_dir: Union[str, pathlib.Path], force_rerun: bool = False):
        """
        docker pip install. Run docker build, docker run with volume mount and zip the output dependencies into a zip
        Because AWS set a size cap of 50MB for lambda layers, we did some size control by removing redundant packages
        like boto3(available by default for any lambda) as well as some dist-info folders

        :param root_dir: str, folder that is mounted with docker
        :param force_rerun: bool, re execute if true, otherwise skip if the folder already exists
        :return: None
        """
        from_folder = os.path.join(root_dir, cls.DEPENDENCY)
        to_file = os.path.join(root_dir, cls.DISDAT_LAYER)
        if not os.path.isdir(from_folder) or force_rerun:
            os.makedirs(from_folder, exist_ok=True)
            # build image from dockerfile, see the dockerfile for more details
            os.system('docker build -t docker_pip:latest {}'.format(os.path.dirname(__file__)))
            # exec container with mounted volume
            os.system('docker run -v {}:/lib/dependency docker_pip:latest'.format(from_folder))
            # zip all dependencies into a zip file
            shutil.make_archive(to_file, format='zip', root_dir=os.path.join(root_dir, cls.DEPENDENCY))
        print('layer zip generated! find it in {}'.format(to_file + '.zip'))


class StateVisitor(states.GraphVisitor):
    def __init__(self, caching: Caching):
        """
        specialized children class of GraphVisitor that determines states to replace on the fly
        :param caching: Caching, we call caching.cache_step() on selected states
        """
        self.to_replace = {}
        self.caching = caching
        super().__init__()

    def visit(self, state):
        """
        for a given state, check if we need to cache it. Note that it only make sense to cache states.Task
        :param state:
        :return:
        """
        self.states[state.state_id] = state
        if isinstance(state, states.Task):
            wrapper = self.caching.cache_step(state)
            self.to_replace[state.state_id] = wrapper
        elif isinstance(state, states.Parallel):
            for b in state.branches:
                b.accept(self)
        elif isinstance(state, states.Map):
            state.iterator.accept(self)


class ExtensiveGraphVisitor(states.GraphVisitor):
    """
    This graph visitor explores the internal structure of Map and Parallel states
    The original GraphVisitor does not support this feature
    """
    def visit(self, state):
        self.states[state.state_id] = state.to_dict()
        if isinstance(state, states.Parallel):
            for b in state.branches:
                b.accept(self)
        elif isinstance(state, states.Map):
            state.iterator.accept(self)
        else:
            pass
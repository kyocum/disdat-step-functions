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


from disdat.common import DisdatConfig
import os
import logging
# TODO FIX THIS IMPORT ONCE THIS PLUGIN IS MERGED INTO disdat
from cache_lambda import PathParam, Cache


try:
    HOME = '/tmp/home'
    os.makedirs(HOME, exist_ok=True)
    os.environ["HOME"] = HOME
    DisdatConfig.init()
except:
    logging.warning("disdat already initialized; home overriding failed")

LOG_LEVEL = logging.INFO + 1


"""
Please copy this file to the lambda function created by you. It must have runtime environment Python>=3.8 
It also need access to Disdat, which is already packaged as a zip for you. Please create a lambda layer 
with the zip file and attach it to this lambda function 
"""


def lambda_handler(event, context):
    logging.log(level=LOG_LEVEL, msg=event)
    cache = Cache(event[PathParam.DSDT_ONLY_ARGS])
    if len(event) == 2:
        # if the input event is a dict of length 2, it's meant for cache_push
        # parent = cache.get_lineage()
        parent = None
        return cache.cache_push(event, parent)
    elif len(event) == 3:
        # if the input event is a dict of length 3, it's meant for cache_pull
        return cache.cache_pull(event)
    else:
        raise ValueError("Input event has invalid length of {}; 2/3 is expected".format(len(event)))
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
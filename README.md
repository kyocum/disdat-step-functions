<img src="docs/logo.png" width="256">

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) 
[![PyPI version](https://badge.fury.io/py/disdat.svg)](https://badge.fury.io/py/disdat) 

## disdat-step-functions
Disdat is a Python (3.6 +) package for data versioning and pipeline authoring that allows data scientists to create
, share, and track data products. Disdat-kfp is a plugin built upon Disdat and enables data versioning for Kubeflow Pipeline (KFP).

More specifically, this plugin does the following:
* **Caching**: Captures all intermediate outputs and reuses cached data based on task parameters.
* **Data Versioning**: All state artifacts are versioned as [bundles](https://disdat.gitbook.io/disdat-documentation/basic-concepts/bundles) on S3;
* **Minimum Intrusion**: Making it easy to refactor existing projects at pipeline level; users don't need to modify any state definitions.
* **Share Datasets**: Intermediary artifacts can be easily shared between teams with [standardized APIs](https://disdat.gitbook.io/disdat-documentation/examples/short-test-drive/push-pull-using-s3).

## Get Started 
Install the package, the pip command will also download the core disdat package if you haven't done so already. 

`pip install disdat-kfp`

Get Started with the tutorial notebook! Check out how easy it is to version a KFP workflow in `simple_cached_workflow.ipynb`

## Documentation
### `caching_wrapper.Caching`
Used to create and configure data versioning parameters that Disdat should use. You can eithe create this object and share it between 
different components (e.g, if all data go to the same location), or you can create one object for each component  
**Args** \
`disdat_context`: `str`, the Disdat context in which the artifacts reside  

`disdat_repo_s3_url`: `str`, url of the S3 bucket. For instance `s3://my-bucket`

`force_rerun_pipeline`: `bool`, force rerun all components if set to `True` 

`use_verbose`: `bool`, see more verbose logs if set to `True` 

`state_machine_name`: `str`, name of the state machine, reserved for future use


### `Caching().cache_step`
Given a user state, wrap it up with dynamically generated states that implements data versioning and 
smart caching. \
**Args** \
`user_step`: The step object(`sepfunctions.steps.states`) to cache 

`bundle_name`: `str`, optional, the name of the bundle to create. Default to state name

`force_rerun`: `bool`, override the pipeline-level `force_rerun`. Set to `True` to enable caching

**Return**  
`stepfunctions.steps.Chain`: a state machine with user's state embedded in


### `PipelineCaching`
Used to refactor an existing pipeline given its definition. `PipelineCaching` finds all `Task` state in the definition 
and call replace it with `Caching().cache_step(task)` (a `steps.Chain` object)

**Args** \
`defintion`: `Union[sepfunctions.steps.states.Chain, sepfunctions.steps.states.State]`, the state machine to refactor 

`caching`: `caching_wrapper.Caching`, used to cache individual states. 


### `PipelineCaching().cache`
Modify state machine `definition` in-place. The state machine now supports data versioning 

**Args** 
`None`

**Return** 
`None`


### `LambdaGenerator.generate`
Generate code and lambda layer that you can use to create a AWS Python Lambda function, which is called by
the augmented state machine to record artifacts.  
**Args**
`root`: `Union[str, pathlib.Path]`, where to dump the generated code 
`force_rerun`: `bool`, re-generate everything and overwrite existing code and zip files

**Return**
`None`

**Artifacts**
This function will generate a folder of artifacts that you can use to create appropriate caching lambda function.
```
Root 
    |- /cache_lambda
    |    -lambda_for_user.py: copy this file to a lambda function
    |    
    |- /dependency: dependencies installed in a amazonlinux container to ensure compatibility
    |
    |- disdat_caching_layer.zip: the lambda layer with all necessary dependencies
```

##  Instrumentation 
Since AWS StepFunction is essentially an orchestrator of tasks with heterogeneous runtimes, we must make sure all tasks, not just Python code, get to enjoy the benefits of data versioning. 
Hence, disdat-step-function injects states before/after user state to pull/push data to S3 (from now on they are called caching steps).  
<img src="docs/instrumentation.png" width="512"> 

To enable data versioning and caching for a state, simply use the `cache_step()` wrapper and pass in the state obj. If you 
have a complex state machine with many states, you can also use `PipelineCaching().cache` to one-button refactor 
the whole workflow. 

As you can see in the figure above, disdat-step-function injects some states around user's designated task. The component name and input parameters 
are used to uniquely identify an execution. Note that you should not use disdat-step-function for tasks that are not idempotent. 

With some high-level understanding of how Disdat-step-function works, let's dive into the actual diagram
<img src="docs/injection.png" width="512"> 


#### `cache_pull_{bundle_name}` step

#### `cache_push_{bundle_name}` step

#### `param_pass_{task_name}` step

#### `param_resolve_{task_name}` step
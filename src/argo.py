import os
from pathlib import Path
from typing import List, Optional, Union

import yaml
from pydantic import BaseModel, Field

from .base import GlobalVar, LocalTarget, Task, to_list
from .s3 import S3Target
from .schedule import (
    create_execution_order,
    schedule,
)


class Metadata(BaseModel):
    generateName: str
    namespace: str = "argo"


class ValueFromPath(BaseModel):
    path: str

class ValueFromSupplied(BaseModel):
    supplied: dict = {}

class Parameter(BaseModel):
    name: str
    value: Optional[str] = None
    valueFrom: Optional[Union[ValueFromPath, ValueFromSupplied, str]] = None

class HttpArtifact(BaseModel):
    url: str

class NameKey(BaseModel):
    name: str
    key: str

class S3Artifact(BaseModel):
    """
    s3:
          endpoint: storage.googleapis.com
          bucket: my-bucket-name
          key: path/in/bucket
          accessKeySecret:
            name: my-s3-credentials
            key: accessKey
          secretKeySecret:
            name: my-s3-credentials
            key: secretKey
    """
    endpoint: Optional[str] = None
    bucket: str
    key: str

    accessKeySecret: Optional[NameKey] = None
    secretKeySecret: Optional[NameKey] = None

class Artifact(BaseModel):
    name: str
    path: Optional[str] = None
    fromm: Optional[str] = Field(alias="from", default=None)
    mode: Optional[str] = None
    http: Optional[HttpArtifact] = None
    s3: Optional[S3Artifact] = None

    # fromExpression: "tasks['flip-coin'].outputs.result == 'heads' ? tasks.heads.outputs.artifacts.result : tasks.tails.outputs.artifacts.result"
    fromExpression: Optional[str] = None
 
class Inputs(BaseModel):
    parameters: Optional[List[Parameter]] = None
    artifacts: Optional[List[Artifact]] = None

class HttpGet(BaseModel):
    path: str
    port: int

class ReadinessProbe(BaseModel):
    httpGet: Optional[HttpGet] = None
    initialDelaySeconds: Optional[int] = None
    timeoutSeconds: Optional[int] = None

class Limits(BaseModel):
    cpu: str
    memory: str

class Resources(BaseModel):
    limits: Optional[Limits] = None
    requests: dict = {}

class Container(BaseModel):
    image: str
    command: List[str]
    args: Optional[List[str]] = None
    readinessProbe: Optional[ReadinessProbe] = None
    daemon: Optional[bool] = None
    resources: Optional[Resources] = None

class Arguments(BaseModel):
    parameters: Optional[List[Parameter]] = None

class DagTaskArguments(Arguments):
    artifacts: Optional[List[Artifact]] = None


class DagTask(BaseModel):
    name: str
    depends: Optional[List[str]] = None # A && B or "A && (C.Succeeded || C.Failed)"
    template: str # template reference
    arguments: DagTaskArguments
    dependencies: List[str] = []
    when: Optional[str] = None #  when: "{{tasks.flip-coin.outputs.result}} == tails"


class Dag(BaseModel):
    tasks: List[DagTask]

class NodeSelector(BaseModel):
    node_name: str = Field(alias="node-name") # node-name: g5xlarge-spot

class Template(BaseModel):
    name: str
    inputs: Optional[Inputs] = None
    container: Optional[Container] = None
    dag: Optional[Dag] = None
    outputs: Optional[Inputs] = None
    nodeSelector: Optional[NodeSelector] = None

class Gauge(BaseModel):
    realtime: bool
    value: str

class PrometheusMetric(BaseModel):
    name: str
    help: str
    labels: List[NameKey] = []
    gauge: Optional[Gauge] = None
    counter: Optional[str] = None
    value: str

class Metrics(BaseModel):
    prometheus: List[PrometheusMetric] = []

class Spec(BaseModel):
    entrypoint: str
    schedule: Optional[str] = None # "*/5 * * * *" # TODO: add cron to workflows
    templates: List[Template]
    metrics: Optional[Metrics] = None
    arguments: Optional[Arguments] = None
    serviceAccountName: str = "argo-workflow"

class Workflow(BaseModel):
    apiVersion: str = "argoproj.io/v1alpha1"
    kind: str = "Workflow"
    metadata: Metadata
    spec: Spec

    def save(self, path: Union[str, Path]):
      print('saving workflow to: ', path)
      workflow_dict = self.model_dump(exclude_none=True, by_alias=True)
      with open(path, "w") as f:
          f.write(yaml.dump(workflow_dict))





"""
Support conditional execution of tasks
tasks:
    - name: flip-coin
      template: flip-coin
    - name: heads
      depends: flip-coin
      template: heads
      when: "{{tasks.flip-coin.outputs.result}} == heads"
    - name: tails
      depends: flip-coin
      template: tails
    when: "{{tasks.flip-coin.outputs.result}} == tails"
"""


"""
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: artifact-passing-
spec:
  entrypoint: artifact-example
  templates:
  - name: artifact-example
    steps:
    - - name: generate-artifact
        template: hello-world-to-file
    - - name: consume-artifact
        template: print-message-from-file
        arguments:
          artifacts:
          # bind message to the hello-art artifact
          # generated by the generate-artifact step
          - name: message
            from: "{{steps.generate-artifact.outputs.artifacts.hello-art}}"

  - name: hello-world-to-file
    container:
      image: busybox
      command: [sh, -c]
      args: ["echo hello world | tee /tmp/hello_world.txt"]
    outputs:
      artifacts:
      # generate hello-art artifact from /tmp/hello_world.txt
      # artifacts can be directories as well as files
      - name: hello-art
        path: /tmp/hello_world.txt

  - name: print-message-from-file
    inputs:
      artifacts:
      # unpack the message input artifact
      # and put it at /tmp/message
      - name: message
        path: /tmp/message
    container:
      image: alpine:latest
      command: [sh, -c]
      args: ["cat /tmp/message"]


apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: output-parameter-
spec:
  entrypoint: output-parameter
  templates:
  - name: output-parameter
    steps:
    - - name: generate-parameter
        template: hello-world-to-file
    - - name: consume-parameter
        template: print-message
        arguments:
          parameters:
          # Pass the hello-param output from the generate-parameter step as the message input to print-message
          - name: message
            value: "{{steps.generate-parameter.outputs.parameters.hello-param}}"

  - name: hello-world-to-file
    container:
      image: busybox
      command: [sh, -c]
      args: ["echo -n hello world > /tmp/hello_world.txt"]  # generate the content of hello_world.txt
    outputs:
      parameters:
      - name: hello-param  # name of output parameter
        valueFrom:
          path: /tmp/hello_world.txt # set the value of hello-param to the contents of this hello-world.txt

  - name: print-message
    inputs:
      parameters:
      - name: message
    container:
      image: busybox
      command: [echo]
      args: ["{{inputs.parameters.message}}"]

apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: dag-diamond-
spec:
  entrypoint: diamond
  templates:
  - name: echo
    inputs:
      parameters:
      - name: message
    container:
      image: alpine:3.7
      command: [echo, "{{inputs.parameters.message}}"]
  - name: diamond
    dag:
      tasks:
      - name: A
        template: echo
        arguments:
          parameters: [{name: message, value: A}]
      - name: B
        dependencies: [A]
        template: echo
        arguments:
          parameters: [{name: message, value: B}]
      - name: C
        dependencies: [A]
        template: echo
        arguments:
          parameters: [{name: message, value: C}]
      - name: D
        dependencies: [B, C]
        template: echo
        arguments:
          parameters: [{name: message, value: D}]
"""




def execute_workflow(workflow: Workflow):
    """ a function to run an argo workflow locally with docker containers """

    # -1: build a template task
    # 0. build a docker image with the ginny loader and necessary dependencies + the workflow code
    # 1. create a graph of tasks and dependencies
    # 2. execute graph in order and docker container

    # Question: how to handle secrets in the workflow?

def get_task_name(task: Task) -> str:
    output_tasks = to_list(task.target())
    input_tasks = to_list(task.depends())
    input_tasks_hash = "-".join([t.id for t in input_tasks])
    output_tasks_hash = "-".join([t.id for t in output_tasks])

    return f"{task.__class__.__name__.lower()}-{input_tasks_hash}-{output_tasks_hash}"

def to_relative(path: Union[str, Path]) -> str:
    path = str(path)
    if path.startswith("/"):
        return path[1:]
    return path

def target_to_artifact(workflow_name: str, task_name: str, target: Union[LocalTarget, S3Target]) -> Artifact:
    if isinstance(target, LocalTarget):
        """
        s3:
          endpoint: s3.amazonaws.com
          bucket: ai-datastore
          region: eu-west-1
          accessKeySecret:
            name: argo-secret
            key: ARGO_WORKFLOWS_ACCESS
          secretKeySecret:
            name: argo-secret
            key: ARGO_WORKFLOWS_SECRET
          key: argo-workflows/test_b/b.txt
        """
        s3 = S3Artifact(
            endpoint="s3.amazonaws.com",
            bucket="ai-datastore",
            key=os.path.join(f"argo-workflows/{workflow_name}/{task_name}", to_relative(target.path)),
            region="eu-west-1", # TODO: based on bucket
            accessKeySecret=NameKey(name="argo-secret", key="ARGO_WORKFLOWS_ACCESS"),
            secretKeySecret=NameKey(name="argo-secret", key="ARGO_WORKFLOWS_SECRET")
        )
        return Artifact(name=target.id, path=target.path, s3=s3)
    
    elif isinstance(target, S3Target):
        return Artifact(name=target.id, 
                        s3=S3Artifact(
                            endpoint="s3.amazonaws.com",
                            bucket=target.bucket, 
                            key=target.path,
                            region="eu-west-1", # TODO: based on bucket
                            accessKeySecret=NameKey(name="argo-secret", key="ARGO_WORKFLOWS_ACCESS"),
                            secretKeySecret=NameKey(name="argo-secret", key="ARGO_WORKFLOWS_SECRET")
                        ))
    else:
        raise Exception(f"invalid target type: {type(target)}")

def get_module_from_class(classA: type) -> str:
    return f"{classA.__module__}.{classA.__name__}"


"""
TODO: using previous step outputs as inputs?

# TODO:
# define all artifacts globally and reuse them as inputs and outputs accordingly.
# know which dependency needs to be shared between tasks and which can be isolated

outputs:
  parameters:
    - name: output-param-1
      valueFrom:
        path: /p1.txt
  artifacts:
    - name: output-artifact-1
      path: /some-directory

dag:
  tasks:
  - name: step-A 
    template: step-template-a
    arguments:
      parameters:
      - name: template-param-1
        value: "{{workflow.parameters.workflow-param-1}}"
  - name: step-B
    dependencies: [step-A]
    template: step-template-b
    arguments:
      parameters:
      - name: template-param-2
        value: "{{tasks.step-A.outputs.parameters.output-param-1}}"
      artifacts:
      - name: input-artifact-1
        from: "{{tasks.step-A.outputs.artifacts.output-artifact-1}}"

"""

def is_target(t: Task) -> bool:
    return isinstance(t, (LocalTarget, S3Target))

def task_to_template(workflow_name: str, t: Task, base_image: str = "python:3.9") -> Template:
    task_name = get_task_name(t)
    # args = " ".join([f"{k} {v}" for k, v in t._get_args().items()])
    # args from parameters of task
    #  "{{inputs.parameters.message}}"

    args = []
    for k, v in t._get_args().items():
        args.append(k)
        args.append("{{" +  f"inputs.parameters.{k}" + "}}")

    # global and local parameters for templates (values get supplied by DAG task)
    input_parameters = [Parameter(name=k, value=None) for k, v in t._get_args().items()]
    outputs = [target_to_artifact(workflow_name, t.id, t) for t in to_list(t.target()) if is_target(t)]
    
    input_artifacts = [target_to_artifact(workflow_name, t.id, t) for t in to_list(t.depends()) if is_target(t)]
    # add input artifacts from related tasks
    for dep in to_list(t.depends()):
        if not is_target(dep) and isinstance(dep, Task):
            dep: Task
            for target in filter(is_target, to_list(dep.target())):
                target: LocalTarget

                # if previous artifact was s3 artifact we need to reference it as an input artifact
                input_artifacts.append(Artifact(name=target.id, path=target.path))
        else:
            input_artifacts.append(target_to_artifact(workflow_name, t.id, dep))

    print("....", task_name, "->", "outputs: ", outputs)
    print("input artifacts: ", input_artifacts)

    command = ["python"]
    command_args = ["-m", "ginny.loader", "--task", get_module_from_class(t.__class__), "--debug", *args]

    task_resources = t.resources()
    resources = Resources(limits=Limits(cpu=task_resources.cpu, memory=task_resources.memory))

    # first template might want to have some input args
    return Template(
        name=f"task-{task_name}",
        outputs=Inputs(artifacts=outputs) if len(outputs) > 0 else None,
        inputs=Inputs(artifacts=input_artifacts, parameters=input_parameters),
        container=Container(image=base_image, command=command, args=command_args, resources=resources),
    )

def task_to_dag_task(workflow_name: str, t: Task):

    parameters = [
        Parameter(name=k, value=v)
        for k, v in t._get_args().items()
        if not isinstance(v, GlobalVar)
    ]

    for k, v in t._get_args().items():
        if isinstance(v, GlobalVar):
            value = "{{workflow.parameters." + k + "}}"
            parameters.append(Parameter(name=k, value=value))

    parameters.append(Parameter(name="__task__", value=get_module_from_class(t.__class__)))

    # recursively resolve dependencies for files of the dependend tasks and add results of those tasks to input artifacts
    input_artifacts = []
    for dep in to_list(t.depends()):
        if not is_target(dep) and isinstance(dep, Task):
            dep: Task
            for target in filter(is_target, to_list(dep.target())):
                from_task = "{{" + f"tasks.{get_task_name(dep)}.outputs.artifacts.{target.id}" + "}}"
                print("from task: ", from_task)
                artifact = Artifact(name=target.id)
                artifact.fromm = from_task
                input_artifacts.append(artifact)
        else:
            input_artifacts.append(target_to_artifact(workflow_name, t.id, dep))
    
    print("input arrtifacts: ", input_artifacts)

    task_name = get_task_name(t)
    dag_task = DagTask(
        name=task_name,
        template=f"task-{task_name}",
        arguments=DagTaskArguments(
            parameters=parameters,
            artifacts=input_artifacts
        ),
        dependencies=[get_task_name(dep) for dep in to_list(t.depends()) if not is_target(dep)],
    )
    return dag_task

def task_to_global_vars(t: Task) -> set[tuple[str, str]]:
    global_vars = set()
    for k, v in t._get_args().items():
        if isinstance(v, GlobalVar):
            global_vars.add((k, v.default))
    return global_vars

def schedule_to_workflow(task: Task, workflow_name: str, base_image: str = "python:3.9") -> Workflow:
    tasks = []
    g = schedule(task, force=True)
    order = create_execution_order(task, g)

    templates = []
    global_vars = set()

    for level, execution_tasks in enumerate(order):
        print("tasks: ", execution_tasks)
        for t in execution_tasks:
            # TOOD: create a hashmap of all tasks (with input args) to see if we have already defined a task or not
            # if we have already defined a task, we can just reference it as a dependency, if not create a task and reference the created one
            # use $taskname_$taskid to create a unique task name (taskid can be a hash of the task input args)
            # when running the task we should just reference the module, input args and the task id
            # a problem: when we have input args from previous results -> this should not happen, as we have strictly defined dependencies with in -and outputs

            templates.append(task_to_template(workflow_name, t, base_image=base_image))
            dag_task = task_to_dag_task(workflow_name, t)
            global_vars.update(task_to_global_vars(t))

            tasks.append(
                dag_task
            )

    dag = Dag(tasks=tasks)
    template_dag = Template(name="dag", dag=dag)
    templates.append(template_dag)

    arguments = Arguments(parameters=[Parameter(name=k, value=v, valueFrom=ValueFromSupplied(supplied={})) for k, v in global_vars])
    spec = Spec(entrypoint="dag", templates=templates, arguments=arguments) # start the dag and not all other templates
    metadata = Metadata(name=workflow_name, generateName=f"{workflow_name}-")
    workflow = Workflow(metadata=metadata, spec=spec)

    return workflow
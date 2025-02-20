import os
from pathlib import Path
from typing import List, Optional, Union

import yaml
from pydantic import BaseModel, Field

from .base import (
    DynamicTask,
    GlobalVar,
    IterableParameterMap,
    LocalTarget,
    Target,
    Task,
    _get_args,
    to_list,
)
from .s3 import S3Target
from .schedule import (
    create_execution_order,
    schedule,
)


class Metadata(BaseModel):
    generateName: Optional[str] = None
    name: Optional[str] = None
    namespace: str = "argo"

class ValueFromPath(BaseModel):
    path: str

class ValueFromSupplied(BaseModel):
    supplied: dict = {}

class Parameter(BaseModel):
    name: str
    value: Optional[str] = None
    valueFrom: Optional[Union[ValueFromPath, ValueFromSupplied, str]] = None
    globalName: Optional[str] = None

class HttpArtifact(BaseModel):
    url: str

class NameKey(BaseModel):
    name: str
    key: str



class S3StorageConfig(BaseModel):
    bucket: str
    region: str
    endpoint: str = "s3.amazonaws.com"
    key: str = "argo-workflows"
    accessKeySecret: NameKey = NameKey(name="argo-secret", key="ARGO_WORKFLOWS_ACCESS")
    secretKeySecret: NameKey = NameKey(name="argo-secret", key="ARGO_WORKFLOWS_SECRET")

    @classmethod
    def from_env(cls):
        # pass
        #    key="argo-workflows"
        #     endpoint="s3.amazonaws.com",
        #     bucket="ai-datastore",
        #     region="eu-west-1", # TODO: based on bucket
        #     accessKeySecret=NameKey(name="argo-secret", key="ARGO_WORKFLOWS_ACCESS"),
        #     secretKeySecret=NameKey(name="argo-secret", key="ARGO_WORKFLOWS_SECRET")
        return cls(
            key=os.getenv("S3_KEY", "argo-workflows"),
            endpoint=os.getenv("S3_ENDPOINT", "s3.amazonaws.com"),
            bucket=os.getenv("S3_BUCKET"),
            region=os.getenv("S3_REGION"),
            accessKeySecret=NameKey(name=os.getenv("S3_ACCESS_KEY_SECRET_NAME", "argo-secret"), key=os.getenv("S3_ACCESS_KEY_SECRET_KEY", "ARGO_WORKFLOWS_ACCESS")),
            secretKeySecret=NameKey(name=os.getenv("S3_SECRET_KEY_SECRET_NAME", "argo-secret"), key=os.getenv("S3_SECRET_KEY_SECRET_KEY", "ARGO_WORKFLOWS_SECRET")),
        )
    @classmethod
    def from_yaml(cls, path: Union[str, Path]):
        data = yaml.load(open(path, "r"), Loader=yaml.FullLoader)
        return cls(**data)

class Limits(BaseModel):
    cpu: str
    memory: str

class Resources(BaseModel):
    limits: Optional[Limits] = None
    requests: dict = {}

class ArgoConfig(BaseModel):
    storage: S3StorageConfig
    namespace: str = "argo"
    serviceAccountName: str = "argo-workflow"
    resources: Resources = Resources(limits=Limits(cpu="1", memory="1Gi"))

    @classmethod
    def from_yaml(cls, path: Union[str, Path]):
        data = yaml.load(open(path, "r"), Loader=yaml.FullLoader)
        args = {}
        if "resources" in data:
            args["resources"] = Resources(**data["resources"])
        if "namespace" in data:
            args["namespace"] = data["namespace"]
        if "serviceAccountName" in data:
            args["serviceAccountName"] = data["serviceAccountName"]

        return cls(
            storage=S3StorageConfig(**data["storage"]), 
            **args
        )

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
    region: str

    accessKeySecret: Optional[NameKey] = None
    secretKeySecret: Optional[NameKey] = None

    @classmethod
    def from_config(cls, config: S3StorageConfig, key: str):
        return cls(
            endpoint=config.endpoint,
            bucket=config.bucket,
            key=key,
            region=config.region,
            accessKeySecret=config.accessKeySecret,
            secretKeySecret=config.secretKeySecret,
        )

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
    withParam: Optional[str] = None # withParam: "{{tasks.date-generator.outputs.parameters.generated_dates}}"

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

def get_task_name(task: Task) -> str:
    return task.__class__.__name__.lower()
    # output tasks of dynamic takss cannnot be defined
    # output_tasks = to_list(task.target()) if not isinstance(task, DynamicTask) else []
    # input_tasks = to_list(task.depends())
    # input_tasks_hash = "-".join([t.id for t in input_tasks])
    # output_tasks_hash = "-".join([t.id for t in output_tasks])

    # return f"{task.__class__.__name__.lower()}-{input_tasks_hash}-{output_tasks_hash}"

def to_relative(path: Union[str, Path]) -> str:
    path = str(path)
    if path.startswith("/"):
        return path[1:]
    return path

def target_to_artifact(workflow_name: str, task_name: str, target: Union[LocalTarget, S3Target], config: S3StorageConfig) -> Artifact:
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
        s3 = S3Artifact.from_config(
            config,
            key=os.path.join(f"{config.key}/{workflow_name}/{task_name}", to_relative(target.path)),
        )
        return Artifact(name=target.id, path=target.path, s3=s3)
    
    elif isinstance(target, S3Target):
        return Artifact(name=target.id, s3=S3Artifact.from_config(config, key=target.path))
    # elif isinstance(target, IterableParameterMap):
    # return Artifact(name=target.name, path=target.local_target.full_path, s3=S3Artifact.from_config(config, key=target.name))
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

def is_parameter_target(t: Task) -> bool:
    return isinstance(t, IterableParameterMap)

def target_to_parameter(target: IterableParameterMap) -> Parameter:
    return Parameter(name=target.name, valueFrom=ValueFromPath(path=str(target.local_target.full_path)), globalName=target.name)

def _to_template(
        workflow_name: str, 
        task_name: str, 
        task_id: str, 
        task_class: object, 
        args: dict,
        targets: List[Target], 
        depends: List[Target], 
        storage_config: S3StorageConfig,
        resources: Resources = Resources(limits=Limits(cpu="1", memory="1Gi")),
        base_image: str = "python:3.9") -> Template:
    # global and local parameters for templates (values get supplied by DAG task)
    input_parameters = [Parameter(name=k, value=None) for k, v in args.items()]
    
    output_artifacts = [target_to_artifact(workflow_name, task_id, t, storage_config) for t in to_list(targets) if is_target(t)]
    output_parameters = [target_to_parameter(target) for target in to_list(targets) if is_parameter_target(target)]
    
    input_artifacts = [target_to_artifact(workflow_name, task_id,  t, storage_config) for t in to_list(depends) if is_target(t)]
    
    # add input artifacts from related tasks
    for dep in to_list(depends):
        if not is_target(dep) and isinstance(dep, Task):
            dep: Task
            for target in filter(is_target, to_list(dep.target())):
                target: LocalTarget

                # if previous artifact was s3 artifact we need to reference it as an input artifact
                input_artifacts.append(Artifact(name=target.id, path=target.path))
        else:
            input_artifacts.append(target_to_artifact(workflow_name, task_id, dep, storage_config))

    command = ["python"]

    loader_args = []
    for k, v in args.items():
        loader_args.append(k)
        loader_args.append("{{inputs.parameters." + k + "}}")

    command_args = ["-m", "ginny.loader", "--task", get_module_from_class(task_class), "--debug", *loader_args]

    # first template might want to have some input args
    return Template(
        name=f"task-{task_name}",
        outputs=Inputs(artifacts=output_artifacts, parameters=output_parameters) if output_artifacts or output_parameters else None,
        inputs=Inputs(artifacts=input_artifacts, parameters=input_parameters),
        container=Container(image=base_image, command=command, args=command_args, resources=resources),
    )

def task_to_template(workflow_name: str, t: Task, config: ArgoConfig, base_image: str = "python:3.9") -> Template:
    task_name = get_task_name(t)
    task_id = t.id
    task_class = t.__class__
    args = t._get_args()
    targets = to_list(t.target())
    depends = to_list(t.depends())
    task_resources = t.resources()
    resources = Resources(limits=Limits(cpu=task_resources.cpu, memory=task_resources.memory)) if task_resources else config.resources

    return _to_template(workflow_name, task_name, task_id, task_class, args, targets, depends, config.storage, resources=resources, base_image=base_image)

def get_paramerized_task_from_dynamic_task(t: DynamicTask) -> Task:
    task_class = t.taskclass
    task_args = _get_args(task_class)
    parameterized_task: Task = task_class(**{k: "{{" + f"inputs.parameters.{k}" + "}}" for k, _ in task_args.items()})
    return parameterized_task


def dynamic_task_to_template(workflow_name: str, t: DynamicTask, config: ArgoConfig, base_image: str = "python:3.9") -> Template:
    task_class = t.taskclass
    assert len(t._get_args()) == 0, "Dynamic tasks should not have any input arguments as those arguments are generated by a dependable task"

    # task_class should not have any depends()
    task_args = _get_args(task_class)
    parameterized_task: Task = get_paramerized_task_from_dynamic_task(t)
    task_name = get_task_name(parameterized_task)

    depends = to_list(parameterized_task.depends())
    targets = to_list(parameterized_task.target())

    task_resources = parameterized_task.resources()
    resources = Resources(limits=Limits(cpu=task_resources.cpu, memory=task_resources.memory)) if task_resources else config.resources

    task_id = parameterized_task.id
    template = _to_template(workflow_name, task_name, task_id, task_class, task_args, targets, depends, config.storage, resources=resources, base_image=base_image)
    return template

def _get_dynamic_task_name(t: DynamicTask) -> str:
    return get_task_name(get_paramerized_task_from_dynamic_task(t))


def task_to_dag_task(workflow_name: str, t: Task) -> DagTask:

    parameters = [
        Parameter(name=k, value=v)
        for k, v in t._get_args().items()
        if not isinstance(v, GlobalVar)
    ]

    with_param = None

    if isinstance(t, DynamicTask):
        # add parameters of the dynamic task

        assert len(to_list(to_list(t._parameter_list))) == 1, "Dynamic tasks should only have one parameter"
        # assert len(to_list(to_list(t.depends()))) == 1, "Dynamic tasks should only have one dependency that generates the parameters"

        for dep_task, param_list in t.parameter.items():
            assert len(param_list) == 1, f"Dynamic tasks should only have one parameter, but {dep_task} has {len(param_list)} of IterableParameterMap"
            param_map = param_list[0]

            for key in param_map.keys:
                parameters.append(Parameter(name=key, value="{{item." + key + "}}"))

            with_param = "{{tasks." + get_task_name(dep_task) + ".outputs.parameters." + param_map.name + "}}"
            break


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

            # limit to 1 level
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

    arguments = DagTaskArguments(
        artifacts=input_artifacts,
        parameters=parameters,
    )

    template = f"task-{task_name}" if not isinstance(t, DynamicTask) else f"task-{_get_dynamic_task_name(t)}"

    dag_task = DagTask(
        withParam=with_param,
        name=task_name,
        template=template,
        arguments=arguments,
        dependencies=[get_task_name(dep) for dep in to_list(t.depends()) if not is_target(dep)],
    )
    return dag_task

def task_to_global_vars(t: Task) -> set[tuple[str, str]]:
    global_vars = set()
    for k, v in t._get_args().items():
        if isinstance(v, GlobalVar):
            global_vars.add((k, v.default))
    return global_vars

def schedule_to_workflow(task: Task, workflow_name: str, config: ArgoConfig, base_image: str = "python:3.9", entrypoint: str = "dag", gen_new_name=True) -> Workflow:
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
            if isinstance(t, DynamicTask) or (hasattr(t, "_is_dynamic") and t._is_dynamic):
                print("converting dynamic task with parameters to template and dag task")

                # create template of taskclass of DynamicTask
                templates.append(dynamic_task_to_template(workflow_name, t, config, base_image=base_image))
                # create dag task that calls the template above
                dag_task = task_to_dag_task(workflow_name, t)
                global_vars.update(task_to_global_vars(t))
            else:
                templates.append(task_to_template(workflow_name, t, config, base_image=base_image))
                dag_task = task_to_dag_task(workflow_name, t)
                global_vars.update(task_to_global_vars(t))

            tasks.append(
                dag_task
            )

    dag = Dag(tasks=tasks)
    template_dag = Template(name=entrypoint, dag=dag)
    templates.append(template_dag)

    arguments = Arguments(parameters=[Parameter(name=k, value=v, valueFrom=ValueFromSupplied(supplied={})) for k, v in global_vars])
    spec = Spec(entrypoint=entrypoint, templates=templates, arguments=arguments) # start the dag and not all other templates
    metadata = Metadata(name=workflow_name if not gen_new_name else None, generateName=f"{workflow_name}-" if gen_new_name else None, namespace=config.namespace)
    workflow = Workflow(metadata=metadata, spec=spec)

    return workflow
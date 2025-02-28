# Ginny

A simple, convenient task manager that is similar to luigi framework but less blown up.
It allows easy exceution and scheduling of tasks locally and remotelty using argo workflows. 

### Run locally

```python
from ginny import DownloadTask, run

result = run(DownloadTask(
    url="https://static.wikia.nocookie.net/harrypotter/images/e/e9/Ginny-HQ-ginevra-ginny-weasley.jpg/revision/latest/scale-to-width-down/250?cb=20150228082608&path-prefix=de", 
    destination='image.jpg')
)
```

### Schedule tasks via command line

```bash
ginny --task ginny.DownloadTask url "https://static.wikia.nocookie.net/harrypotter/images/e/e9/Ginny-HQ-ginevra-ginny-weasley.jpg/revision/latest/scale-to-width-down/250?cb=20150228082608&path-prefix=de" destination "image.jpg" 

# run every 5 minutes
ginny --task ginny.DownloadTask --every 'minute' --count 5 url "https://static.wikia.nocookie.net/harrypotter/images/e/e9/Ginny-HQ-ginevra-ginny-weasley.jpg/revision/latest/scale-to-width-down/250?cb=20150228082608&path-prefix=de" destination "image.jpg"

# EVERY DAY at 0:00
ginny --task ginny.DownloadTask --every 'day' --at "00:00" url "https://static.wikia.nocookie.net/harrypotter/images/e/e9/Ginny-HQ-ginevra-ginny-weasley.jpg/revision/latest/scale-to-width-down/250?cb=20150228082608&path-prefix=de" destination "image.jpg" 
```

#### Build your own tasks

```python
from ginny import run, Task
import dataclasses

@dataclasses.dataclass(frozen=True)
class MyTask(Task):
    url: str

    def depends(self):
        # return tasks or targets that this task depends on
        # return LocalTarget("/tmp/data.json")
        # return [LocalTarget("/tmp/data.json"), LocalTarget("/tmp/data2.json")]
        return [LocalTarget("/tmp/data.json"), DownloadTask(self.url, "/tmp/data2.json")]
    
    def run(self, *args, **kwargs):
        target, download_task = self.depends()
        data1 = target.read_json()
        data2 = download_task.target().read_json()
        data1.update(data2)

        with self.target().open("w") as writer:
            writer.write("done")
        
    def target(self):
        # define a target if the task should not be executed every time / has output data
        return LocalTarget("/tmp/target.json")

# run the task (results of all tasks that will be executed are returned in results)
task = MyTask(url=...)

# delelte results of tasks
task.delete(recursive=False) # set recursive=True, to also delete results of subtasks

results = run(task)
```


### Buld-in tasks
```python
from ginny import BashTask, S3DownloadTask, DownloadTask, S3UploadTask, Task, SSHCommandTask, DepTask, TempDownloadTask, run

r = run(BashTask(['ls', '-lha']))
```

### Run Dag/Task with Argo Workflows (local targets will automatically become s3 targets)

Define argo config with storage via yaml (preferred) and save as `storage.yaml` or use `.from_env()` to load from environment vars

```yaml (argo_config.yaml)
namespace: "argo" # default
serviceAccountName: "argo-workflows" # default

storage:
    key: "argo-workflows" # default
    bucket: "ai-datastore" # required
    region: "us-east-1" # required
    endpoint: "s3.amazonaws.com" # default

    accessKeySecret: # default
        name: "argo-secret"
        key: "ARGO_WORKFLOWS_ACCESS"

    secretKeySecret: # default
        name: "argo-secret"
        key: "ARGO_WORKFLOWS_SECRET2"
```
Define tasks:

```python
import dataclasses
from typing import List

from src import GlobalVar, LocalTarget, Task, S3StorageConfig

@dataclasses.dataclass(frozen=True)
class A(Task):
    pano_id: str
    order_id: str = GlobalVar("order_id")

    def run(self, *args, **kwargs):
        self.target().write_text("hello")

    def target(self):
        return LocalTarget("/tmp/a.txt")

@dataclasses.dataclass(frozen=True)
class B(Task):
    def run(self, *args, **kwargs):
        self.target().write_text("hello")

    def target(self):
        return LocalTarget("/tmp/b.txt")

# define the workflow (allows to define global variables which are necessary to make the workflow run)
@dataclasses.dataclass(frozen=True)
class Pipeline(Task):
    order_id: str = GlobalVar("order_id")

    def depends(self) -> List[Task]:
        a = A(order_id=self.order_id, pano_id="testing123")
        b = B()
        return [a, b]

    def run(self, *args, **kwargs):
        print("Running pipeline")
        data1 = self.depends()[0].target().read_text()
        print("Task A exists: ", self.depends()[0].target().exists())
        print("Task A result: ", data1)
        data2 = self.depends()[1].target().read_text()
        print("Task B exists: ", self.depends()[1].target().exists())
        print("Task B result: ", data2)
        print("Total result: ")

        print(data1 + data2)
```

Create the workflow yaml from the task
```python
### export the task graph as a workflow
task = Pipeline()
config = ArgoConfig.from_yaml("argo_config.yaml")

# use the base image here where your workflow will be defined and that has the requirements (ginny) installed
workflow = schedule_to_workflow(task, "a-b-process-test", config, base_image="baudcode/ginny_test:latest") 
workflow.save("test_workflow.yaml")
```
Push test_workflow.yaml to argo workflows
```bash
argo submit -n argo --watch test-workflow.yaml
```

### Run dynamic tasks

Limit: Dynamic tasks are not allowed to have another dynamic task dependecy. 

```python
# generate some parameters within some task (producer)
@dataclasses.dataclass(frozen=True)
class GenerateLines(Task):
    def run(self, *args, **kwargs):
        self.target()[2].set([
            {"key": "testing123", "dummy": "1"},
            {"key": "testing456", "dummy": "2"},
            {"key": "testing4567", "dummy": "3"},
        ])

    def target(self):
        return [IterableParameterMap(name='data', keys=['key', 'dummy'])]

# consume one item
@dataclasses.dataclass(frozen=True)
class ProcessLine(Task):
    key: str
    dummy: str

    def run(self, *args, **kwargs):
        self.target().write_text(f"processed {self.key} {self.dummy}")
    
    def target(self):
        return LocalTarget(f"/tmp/processed_{self.key}.txt")

# run all in parallel
@dataclasses.dataclass(frozen=True)
class ProcessLines(DynamicTask):

    @property
    def taskclass(self):
        return ProcessLine
    
    @property
    def parameter(self):
        return [IterableParameterMap(name='data', keys=['pano_id', 'order_id'])]

    def depends(self):
        return [GenerateLines()]
```

### Connect task to argo events

```bash
WIP
```


### Development

```bash

python setup.py clean
pip install .
```

### TODO

- implement argo events and argo sensors to connect tasks to them and make it possible to simulate events comming from them
- use logging
- make dynamic tasks work with argo workflows
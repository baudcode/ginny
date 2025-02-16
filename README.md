# Ginny

A simple, convenient task manager that is similar to luigi framework but less blown up.
It allows easy exceution and scheduling of tasks locally and remotelty. 

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

### Run Dag/Task with Argo Workflows

```

```


### Run dynamic tasks

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

- run complete pipelines remotely
- add gpu support for running remotely
- limit resoures to run tasks
- use logging
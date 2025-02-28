import base64
import copy
import dataclasses
import datetime
import hashlib
import itertools
import json
import logging
import os
import shutil
import subprocess
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor as ProcessPool
from concurrent.futures import ThreadPoolExecutor as ThreadPool
from pathlib import Path
from timeit import default_timer
from typing import Callable, Dict, List, Optional, Tuple, Union

from .docker import Container
from .utils import download, logger


# Define the GlobalVar type
class GlobalVar(str):
    # def __new__(cls, value, default=None):
    # return str.__new__(cls, default) if value is not None else None
    
    # how can I test that default was set?
    def __init__(self, value, default=None):
        self.default = default
    
    def __str__(self):
        return str(self.default)

class Uri:
    def __init__(self, uri: str) -> None:
        self.uri = uri

    def __repr__(self):
        return self.uri

    def __hash__(self) -> int:
        return hash(self.uri)

    def __eq__(self, __o: object) -> bool:
        return __o.uri == self.uri

    def __str__(self):
        return f"<Uri uri={self.uri}>"


def encode(url: str):
    return str(base64.urlsafe_b64encode(bytes(url, "utf-8")), 'utf-8')


def encode_short(url: str):
    e = hashlib.sha1(bytes(url, 'utf-8'))
    return e.hexdigest()

def _get_args(c: object):
    args = {}
    for k in c.__dataclass_fields__.keys():
        if k.startswith("_"):
            continue
        
        try:
            value = getattr(c, k)
        except AttributeError as e:
            value = None

        args[k] = value
    return args

@dataclasses.dataclass(frozen=True)
class Comparable:
    # def _get_args(self):
    #     valid_items = filter(lambda x: not x[0].startswith("_"), self.__dict__.items())
    #     return ",".join(list(map(lambda x: F"{x[0]}={x[1]}", valid_items)))

    @property
    def id(self):
        encoded_args = hashlib.blake2b(self._get_args_str().encode(), digest_size=8).hexdigest()
        return self.__class__.__name__.lower() + "-" + encoded_args

    def _get_args(self):
        return _get_args(self)
    
    def _get_args_str(self):
        return  ",".join(list(map(lambda x: F"{x[0]}={x[1]}", self._get_args().items())))

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} {self._get_args_str()}>"

    def __hash__(self) -> int:
        return hash(self.__repr__())

    def __eq__(self, __o: "Task") -> bool:
        return hash(__o) == hash(self)

@dataclasses.dataclass(frozen=True)
class Target(Comparable):

    def exists(self) -> bool:
        """ checks whether or not the target exists """
        raise NotImplementedError()

    def delete(self):
        """ deletes the target """
        raise NotImplementedError()


@dataclasses.dataclass(frozen=True)
class LocalTarget(Target):
    path: Union[str, Path]

    def __post_init__(self):
        path = Path(self.path)
        if path.is_dir():
            raise ValueError(f"target {path} must bot be a directory")
        
        path.parent.mkdir(parents=True, exist_ok=True)


    @property
    def full_path(self):
        return Path(self.path).absolute()

    def exists(self):
        return self.full_path.exists()

    def open(self, mode: str, **args):
        return self.full_path.open(mode=mode, **args)

    def delete(self):
        return self.full_path.unlink(missing_ok=True)

    def read_text(self):
        with self.open("r") as reader:
            data = reader.read()

        return data

    def write_text(self, text: str):
        with self.open("w") as writer:
            writer.write(text)

    def write_json(self, data: dict):

        with self.open('w') as writer:
            writer.write(json.dumps(data))

    def read_image(self, pil: bool = False):
        import numpy as np
        from PIL import Image

        if pil:
            return Image.open(self.path)
        else:
            return np.asarray(Image.open(self.full_path))

    def read_json(self):
        return json.load(self.full_path.open('r'))

    def md5(self):
        hash_md5 = hashlib.md5()
        with self.full_path.open("rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)

        return hash_md5.hexdigest()

    def copy(self, destination: Union[str, Path, "LocalTarget"], exist_ok: bool = True):

        if isinstance(destination, LocalTarget):
            destination_path = destination.path
        else:
            destination_path = Path(destination)

        if destination_path.exists() and not exist_ok:
            raise FileExistsError(f"destination {destination} already exists. [Copy {self.full_path}]")

        if not self.full_path.exists():
            raise FileExistsError(f"file {self.full_path} already exists. [Copy to {destination_path}")

        shutil.copyfile(self.full_path, destination_path)

        if isinstance(destination, LocalTarget):
            return destination
        else:
            return LocalTarget(destination_path)


OutputType = Union[Target, List[Target], Dict[str, Target]]
Dependency = Union[Target, List[Target], "Task", List["Task"], Dict[str, Target], Dict[str, "Task"]]

def flatten(li: list):
    """ reduces a multi-dimensional list to a single dimension """
    for elem in li:
        if isinstance(elem, list):
            yield from flatten(elem)
        else:
            yield elem



def to_list(o: Optional[OutputType]):
    if o is None:
        return []

    elif isinstance(o, dict):
        li = list(o.values())
    elif isinstance(o, list):
        li = o
    elif isinstance(o, tuple):
        li = list(o)
    else:
        li = [o]

    return list(flatten(li))

def depedendencies_resolved(deps: Dependency) -> bool:
    deps = to_list(deps)

    if len(deps) == 0:
        return True

    return all(o.exists() if isinstance(o, Target) else o.done() for o in deps)

@dataclasses.dataclass
class TaskResources:
    """ resources required for a task (used by argo workflow) """
    cpu: str
    memory: str

def is_task(o: any) -> bool:
    return hasattr(o, "__call__") and hasattr(o, "target") and hasattr(o, "depends") and hasattr(o, "run") and hasattr(o, "done") and hasattr(o, "runnable")       

@dataclasses.dataclass(frozen=True)
class Task(Comparable):

    def resources(self) -> Optional[TaskResources]:
        return None

    def depends(self) -> Dependency:
        return []

    def __call__(self):
        return self.run()

    def run(self, *args, **kwargs): 
        """ run the task. write to the target """
        raise NotImplementedError(f"task {self.__class__.__name__} does not implement run() method")

    def target(self) -> OutputType:
        """ task must not need a target, but task will always be exectued if the target is not defined"""
        return None

    def done(self):
        target = self.target()
        if target is None:
            return False
        
        print("target: ", target)
        return all(o.exists() if not isinstance(o, Task) else o.done() for o in to_list(target))

    def runnable(self) -> bool:
        return depedendencies_resolved(self.depends())

    def unresolved_dependencies(self):
        for dep in to_list(self.depends()):
            if isinstance(dep, Task):
                if not dep.done():
                    yield dep
            elif isinstance(dep, LocalTarget):
                if not dep.exists():
                    yield dep

    def delete(self, recursive: bool = False):
        for t in to_list(self.target()):
            t.delete()

        if recursive:
            for dep in to_list(self.depends()):
                if isinstance(dep, Task):
                    dep.delete(recursive=recursive)

    # def remote(self, ip: str, username: str, workdir: Union[str, Path], pem: Optional[Union[str, Path]] = None, executable: str = "python3"):
    #     client = ssh.get_client(ip, username, pem=pem)

    #     copy_task = copy.deepcopy(self)

    #     # sync dependencies
    #     for dep in to_list(copy_task.depends()):
    #         if isinstance(dep, Target):
    #             # sync local target to remote target
    #             targets = [dep]
    #         elif isinstance(dep, Task):
    #             targets = to_list(dep.target())
    #         else:
    #             targets = []

    #         for target in targets:
    #             if isinstance(target, LocalTarget):
    #                 if not target.exists():
    #                     raise Exception(f"could not find dependency {target}")

    #                 remote_target = LocalTarget(str(target.path.absolute()) + ".copy")
    #                 logger.debug(f"=> syncing target {target} to {remote_target}")
    #                 ssh.upload_file(client, target.path, remote_target.path)
    #                 target.path = remote_target.path

    #             elif isinstance(target, S3Target):
    #                 logger.debug(f"not syncing target {target}")

    #     # execute command python pickled task remotly

    #     # recreate environment
    #     # _, _, stderr = client.exec_command(f"ls {environment}")
    #     # environment_exists = len(stderr.readlines()) == 0

    #     # if not environment_exists:
    #     #     # create environment
    #     #     print("environment does not exist")

    #     # serialize object pickle
    #     path = f"/tmp/remote_task_{self.__class__.__name__}__{encode_short(self._get_args())}.pkl"
    #     remote_path = f"/tmp/task_{self.__class__.__name__}__{encode_short(copy_task._get_args())}.pkl"
    #     remote_result_path = remote_path + '.result'

    #     import os
    #     import pickle

    #     with open(path, 'wb') as writer:
    #         pickle.dump(copy_task, writer)

    #     assert (os.path.getsize(path) > 0)

    #     # upload serialized payload
    #     ssh.upload_file(client, path, remote_path)

    #     command = f"cd {workdir} && {executable} -c \"import pickle as p; task = p.load(open('{remote_path}', 'rb')); r=task.run(); p.dump(r,open('{remote_result_path}', 'wb'))\""
    #     logger.info("execute command: ", command)

    #     _, stdout, stderr = client.exec_command(command, get_pty=True, environment=os.environ)

    #     stderr = stderr.readlines()
    #     stdout = stdout.readlines()

    #     if len(stderr) > 0:
    #         raise Exception(f"failed to execute task remotely. stderr: {stderr}. {stdout}")

    #     # copy result from remote to local machine
    #     logger.debug("copy targts from remote to local")

    #     for target in to_list(self.target()):
    #         if isinstance(target, LocalTarget):
    #             local_path = target.path
    #             # local_path = str(target.path.absolute()) + ".remote"
    #             logger.info(f"download file {local_path}")
    #             ssh.download_file(client, target.path, local_path)

    #     # copy and read result
    #     local_result_path = remote_result_path + '.local'
    #     ssh.download_file(client, remote_result_path, local_result_path)

    #     return pickle.load(open(local_result_path, 'rb'))

    def _create_simple_local_target(self):
        args = self._get_args()
        return LocalTarget(f"/tmp/{self.__class__.__name__}_{encode_short(args)}.output")

@dataclasses.dataclass(frozen=True)
class DepTask(Task):
    """ a task that depends on other tasks """
    def run(self, *args, **kwargs): 
        pass

    def target(self) -> OutputType:
        return [dep.target() for dep in to_list(self.depends())]

@dataclasses.dataclass(frozen=True)
class DownloadTask(Task):
    """ a task that downloads a file from a url """

    url: str
    destination: Path
    auth: Optional[Tuple[str, str]] = None
    headers: Optional[Dict[str, str]] = None

    def __post_init__(self):
        self.destination = Path(self.destination)

    def run(self, *args, **kwargs): 
        download(self.url, str(self.destination.absolute()), auth=self.auth, headers=self.headers)

    def target(self) -> LocalTarget:
        return LocalTarget(self.destination)

@dataclasses.dataclass(frozen=True)
class TempDownloadTask(Task):

    url: str
    auth: Optional[Tuple[str, str]] = None
    headers: Optional[Dict[str, str]] = None
    suffix: Optional[str] = None
    _destination: Path = dataclasses.field(init=False)

    def __post_init__(self):
        filename = str(get_hash(self.url))
        if self.suffix:
            filename += self.suffix

        self._destination: Path = Path("/tmp/", filename).absolute()

    def run(self, *args, **kwargs):
        logger.debug(f"downloading {self.url} to {self._destination}")
        start = default_timer()
        download(self.url, str(self._destination.absolute()), auth=self.auth, headers=self.headers)
        return dict(elapsed=default_timer() - start)

    def target(self) -> LocalTarget:
        return LocalTarget(self._destination)


def get_hash(obj: any) -> str:
    return encode_short(json.dumps(obj))

@dataclasses.dataclass(frozen=True)
class BashTask(Task):
    cmd: List[str]

    def run(self, *args, **kwargs):
        logger.debug(f"[Bash] executing {self.cmd}")
        result = subprocess.Popen(self.cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        stderr = result.stderr.readlines()
        stdout = result.stdout.readlines()

        if (result.returncode is not None and result.returncode != 0) or len(stderr) > 0:
            raise Exception(f"task {self.__repr__()} [code={result.returncode}] has failed: {stderr}")

        logger.debug(f'stdout: {stdout}')
        logger.debug(f'stderr: {stderr}')

        with self.target().open('wb') as writer:
            writer.writelines(stdout)

        return stdout

    def target(self):
        return LocalTarget(f"/tmp/task_bash_{get_hash(self.cmd)}.output")

@dataclasses.dataclass(frozen=True)
class SingleOutputTask(Task):

    def _run(self) -> Union[str, List[str]]:
        pass

    def run(self, *args, **kwargs):
        output = self._run()

        with self.target().open("w") as writer:
            if isinstance(output, str):
                writer.write(output)
            else:
                writer.writelines(output)

        return output

    def target(self):
        args = self._get_args()
        return LocalTarget(f"/tmp/{self.__class__.__name__}_{encode_short(args)}.output")

class ContainerRun():

    def run(self, *args, **kwargs):
        result = self._container.run(raise_exit_code_nonzero=self.raise_exit_code_nonzero)
        return result.json()

@dataclasses.dataclass(frozen=True)
class DockerContainerTask(SingleOutputTask, ContainerRun):

    name: str
    force: bool = False
    raise_exit_code_nonzero: bool = True
    _container: Container = dataclasses.field(init=False)

    def __post_init__(self):
        self._container = Container.from_name(self.name)


def _create_tmp_path(uri: str):
    ext = os.path.splitext(uri)[1]
    if "?" in ext:
        ext = ext.split("?")[0]
    ext = ext or ".output"
    return Path(f"/tmp/{encode_short(uri)}{ext}")

@dataclasses.dataclass(frozen=True)
class CopyTask(Task):
    source: Union[str, Path]
    destination: Union[str, Path]

    def depends(self):
        return LocalTarget(self.source)

    def run(self, *args, **kwargs):
        source = self.depends()
        target = self.target()

        if target.exists() and source.md5() == target.md5():
            logger.debug(f'copy {self.source} -> {self.destination} | target already exists with same hash')
            return dict(exists=True)
        else:
            source.copy(target)
            return dict(exists=False)

    def target(self):
        return LocalTarget(self.destination)

@dataclasses.dataclass(frozen=True)
class DownloadAnyTask(DepTask):

    def __init__(self, uri: str, local_path: Union[str, Path] = None, **download_args):
        self.uri = uri
        self.download_args = download_args
        self.local_path = Path(local_path) if local_path else _create_tmp_path(uri)

    def depends(self):
        """ test uri format """
        if self.uri.startswith("s3://"):
            from src.s3 import S3DownloadTask
            return S3DownloadTask(self.uri, self.local_path, **self.download_args)
        elif self.uri.startswith("http://") or self.uri.startswith("https://"):
            return DownloadTask(self.uri, self.local_path, **self.download_args)
        else:
            return CopyTask(self.uri, self.local_path)

    def target(self):
        return LocalTarget(self.local_path)

@dataclasses.dataclass(frozen=True)
class LambdaTarget(Target):

    def __init__(self, lambda_fn: Callable):
        self.lambda_fn = lambda_fn

    def exists(self) -> bool:
        return self.lambda_fn()

    def delete(self):
        pass

@dataclasses.dataclass(frozen=True)
class MultiTask(Task):
    def __init__(self, tasks: List[Task]) -> None:
        self.tasks = tasks
    
    def depends(self):
        return self.tasks

@dataclasses.dataclass(frozen=True)
class MLDockerTask(ContainerRun):

    container_name: str
    inputs: List[str]
    outputs: List[str]


    _container: Container = dataclasses.field(init=False, default=None)
    _input_path: Container = dataclasses.field(init=False, default=None)
    _output_path: Container = dataclasses.field(init=False, default=None)
    force: bool = False

    def __post_init__(self):
        self._container = Container.from_name(self.container_name)

        # get input and output path
        logger.info(self._container.mounts)
        self._input_path = list(filter(lambda m: m.destination == Path('/input'), self._container.mounts))[0].source
        self._output_path = list(filter(lambda m: m.destination == Path('/output'), self._container.mounts))[0].source

        # clear mounts
        def clear(dir: str):
            shutil.rmtree(dir, ignore_errors=True)
            os.makedirs(dir, exist_ok=True)

        clear(self._input_path)
        clear(self._output_path)

    def depends(self):
        return [
            DownloadAnyTask(uri, self._input_path / os.path.basename(uri)) for uri in self.inputs
        ] + [
            LambdaTarget(lambda: os.path.exists(self._output_path)),
            LambdaTarget(lambda: os.path.exists(self._input_path)),
        ]

    def run(self, *args, **kwargs):
        return self._container.run(raise_exit_code_nonzero=True)

    def target(self):
        return [LocalTarget(self._output_path / o) for o in self.outputs]
    
@dataclasses.dataclass(frozen=True)
class ParameterTarget(Target):
    """ a wrapper class that can generate a parameter for the next task """
    task: Task = None
    parameters: Dict[str, Union[str, int, float]] = dataclasses.field(default_factory=lambda: {})

    def env_key(self):
        return f"TARGET_PARAMETERS_{self.task.id}"

    def exists(self) -> bool:
        return os.environ.get(self.env_key) is not None
    
    def set(self, key, value):
        self.parameters[key] = value
        os.environ[self.env_key] = json.dumps(self.parameters)

    def delete(self):
        self.parameters.clear()
        os.environ.pop(self.env_key)


@dataclasses.dataclass(frozen=True)
class IterableParameter(Target):
    name: str
    """ a wrapper class that can generates parameters for the next task
    Use exported parameters from one task to execute N next tasks on all parameters

    Example:

    Some task has to write a file with a list of dates. The next task has to read the file and execute a task for each date. 
    ```yaml
        ...
        out_file = open("generated_dates.json", "w") 
        json.dump([{"partition": (startDt + timedelta(days=i)).strftime('%Y-%m-%d')} for i in range(delta+1)],out_file)     
        out_file.close() 
    outputs:
        parameters:
        - name: generated_dates
          valueFrom:
            # default: '[{"partition": "2023-12-20"}]'
            path: generated_dates.json
          globalName: generated_dates
    ```
    The next task will be executed for each date in the list. 
    ```yaml
    - name: data-processing
      depends: "date-generator"
      template: whalesay
      arguments:
        parameters:
        - {name: message, value: "{{item.partition}}"}
      withParam: "{{tasks.date-generator.outputs.parameters.generated_dates}}"
    ```
    """

    @property
    def local_target(self):
        return LocalTarget(f"/tmp/iterable_parameter_{self.name}.json") # TODO: change from abs path

    def exists(self) -> bool:
        return self.local_target.exists()
    
    def set(self, values: List[Union[str, int, float]]):
        data = [{self.name: v} for v in values]
        self.local_target.write_json(data)
    
    def values(self):
        data = self.local_target.read_json() # returns a list of parameters
        return [d[self.name] for d in data]
    
    def list(self):
        return self.local_target.read_json()
    
    def dict(self):
        return {self.name: self.values()}

@dataclasses.dataclass(frozen=True)
class IterableParameterMap(Target):
    name: str
    keys: List[str]

    @property
    def local_target(self):
        return LocalTarget(f"/tmp/iterable_parameter_map_{self.name}.json") # TODO: change from abs path

    def exists(self) -> bool:
        return self.local_target.exists()

    def list(self) -> List[dict]:
        # returns a list of dictionaries (parameters for each task)
        return self.local_target.read_json()
    
    def set(self, data: List[Dict[str, Union[str, int, float]]]):
        print(f"setting {data} to {self.name} with keys {self.keys}")
        assert all(set(d.keys()) == set(self.keys) for d in data)
        self.local_target.write_json(data)
    
    def dict(self):
        l = self.list()
        keys = l[0].keys()
        return {k: [d[k] for d in l] for k in keys}


class DynamicTask(Task):
    _is_dyanmic = True

    @property
    def parameter_map(self) -> Optional[str]:
        """ need to define parameter_map property if you want to use it, otherwise first parameter map of depending task will be used """
        return None

    @property
    def taskclass(self):
        raise NotImplementedError(f"need to define taskclass property for {self.__class__.__name__}")
    
    @property
    def parameter(self) -> Dict[Task, List[IterableParameter]]:
        # we must depend on tasks which have parameter maps as targets
        depends = to_list(self.depends())

        # returns all iterable parameter maps it can find in the dependend tasks
        parameters = defaultdict(lambda: [])

        for d in filter(is_task, depends):
            d: Task
            for t in to_list(d.target()):
                if isinstance(t, IterableParameterMap):
                    if self.parameter_map is not None and t.name != self.parameter_map:
                        continue

                    parameters[d].append(t)

        if len(parameters) == 0:
            raise Exception(f"task {self.__class__.__name__} must have a dependent task which returns an IterableParameterMap")
        
        return parameters
    
    @property
    def _parameter_list(self):
        return [k for t in self.parameter.values() for k in t]

    def run(self, pool: ThreadPool, *args, **kwargs): 
        tasks = self._runnable_tasks()

        # create a local execution order
        from . import run
        print("running tasks", tasks)
        return [run(t, pool=pool.__class__) for t in tasks]

    def done(self):
        iterable_targets = to_list(self._parameter_list)
        if not all(t.exists() for t in iterable_targets):
            return False
        return super().done()

    def _tasks(self, debug=False):
        target: List[Union[IterableParameter, IterableParameterMap]] = to_list(self._parameter_list)
        
        data = {}
        for t in target:
            data.update(t.dict())
        
        params = [{k: v[i] for k, v in data.items()} for i in range(len(data[list(data.keys())[0]]))]
        print("params: ", params)
        # print("data: ", data)
        tasks: List[Task] = [self.taskclass(**params[i]) for i in range(len(params))]
        # print("tasks: ", tasks)
        return tasks
    
    def generate(self):
        return self._tasks()

    def _runnable_tasks(self, debug=False) -> List[Task]:
        tasks = self._tasks(debug=debug)
        if debug: print(f"trying to schedule tasks {tasks}")
        if debug: print("checking which tasks to run")
        tasks = [t for t in tasks if t.runnable() and not t.done()]
        if debug: print(f"running tasks {tasks}")
        return tasks

    def target(self):
        return self._tasks()
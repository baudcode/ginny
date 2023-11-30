from typing import Callable
import copy
import logging
import io
from timeit import default_timer
from abc import ABC
from . import s3
from typing import List, Dict, Union, Optional, Tuple, runtime_checkable, Protocol
from pathlib import Path
from .utils import download
from . import ssh
import subprocess
import json
from PIL import Image
import numpy as np
import base64
import hashlib
from .docker import Container
import os
import shutil
from .utils import logger


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


@runtime_checkable
class Comparable(Protocol):
    def _get_args(self):
        valid_items = filter(lambda x: not x[0].startswith("_"), self.__dict__.items())
        return ",".join(list(map(lambda x: F"{x[0]}={x[1]}", valid_items)))

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} {self._get_args()}>"

    def __hash__(self) -> int:
        return hash(self.__repr__())

    def __eq__(self, __o: "Task") -> bool:
        return hash(__o) == hash(self)


class Target(Comparable):
    def exists(self) -> bool:
        """ checks whether or not the target exists """
        raise NotImplementedError()

    def delete(self):
        """ deletes the target """
        raise NotImplementedError()


class S3Target(s3.S3File):

    def delete(self):
        return super().unlink()

    @classmethod
    def from_uri(cls, uri: str):
        bucket, path = s3.S3.split_uri(uri)
        return S3Target(bucket, path)

    def __repr__(self) -> str:
        return


class LocalTarget(Target):
    def __init__(self, path: Union[str, Path], *args):
        self.path = Path(path, *args).absolute()

    def exists(self):
        return self.path.exists()

    def open(self, mode: str, **args):
        return self.path.open(mode=mode, **args)

    def delete(self):
        return self.path.unlink(missing_ok=True)

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
        if pil:
            return Image.open(self.path)
        else:
            return np.asarray(Image.open(self.path))

    def read_json(self):
        return json.load(self.path.open('r'))

    def md5(self):
        hash_md5 = hashlib.md5()
        with self.path.open("rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)

        return hash_md5.hexdigest()

    def copy(self, destination: Union[str, Path, "LocalTarget"], exist_ok: bool = True):

        if isinstance(destination, LocalTarget):
            destination_path = destination.path
        else:
            destination_path = Path(destination)

        if destination_path.exists() and not exist_ok:
            raise FileExistsError(f"destination {destination} already exists. [Copy {self.path}]")

        if not self.path.exists():
            raise FileExistsError(f"file {self.path} already exists. [Copy to {destination_path}")

        shutil.copyfile(self.path, destination_path)

        if isinstance(destination, LocalTarget):
            return destination
        else:
            return LocalTarget(destination_path)


OutputType = Union[Target, List[Target], Dict[str, Target]]
Dependency = Union[Target, List[Target], "Task", List["Task"], Dict[str, Target], Dict[str, "Task"]]


def to_list(o: Optional[OutputType]):
    if o is None:
        return []

    elif isinstance(o, dict):
        return list(o.values())
    elif isinstance(o, list):
        return o
    elif isinstance(o, tuple):
        return list(o)
    else:
        return [o]


def depedendencies_resolved(deps: Dependency) -> bool:
    deps = to_list(deps)

    if len(deps) == 0:
        return True

    return all(o.exists() if isinstance(o, Target) else o.done() for o in deps)


class Task(Comparable):
    def depends(self) -> Dependency:
        return []

    def run(self):
        """ run the task. write to the target """
        raise NotImplementedError(f"task {self.__class__.__name__} does not implement run() method")

    def target(self) -> OutputType:
        """ task must not need a target, but task will always be exectued if the target is not defined"""
        return None

    def done(self):
        return all(o.exists() for o in to_list(self.target()))

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

    def remote(self, ip: str, username: str, workdir: Union[str, Path], pem: Optional[Union[str, Path]] = None, executable: str = "python3"):
        client = ssh.get_client(ip, username, pem=pem)

        copy_task = copy.deepcopy(self)

        # sync dependencies
        for dep in to_list(copy_task.depends()):
            if isinstance(dep, Target):
                # sync local target to remote target
                targets = [dep]
            elif isinstance(dep, Task):
                targets = to_list(dep.target())
            else:
                targets = []

            for target in targets:
                if isinstance(target, LocalTarget):
                    if not target.exists():
                        raise Exception(f"could not find dependency {target}")

                    remote_target = LocalTarget(str(target.path.absolute()) + ".copy")
                    logger.debug(f"=> syncing target {target} to {remote_target}")
                    ssh.upload_file(client, target.path, remote_target.path)
                    target.path = remote_target.path

                elif isinstance(target, S3Target):
                    logger.debug(f"not syncing target {target}")

        # execute command python pickled task remotly

        # recreate environment
        # _, _, stderr = client.exec_command(f"ls {environment}")
        # environment_exists = len(stderr.readlines()) == 0

        # if not environment_exists:
        #     # create environment
        #     print("environment does not exist")

        # serialize object pickle
        path = f"/tmp/remote_task_{self.__class__.__name__}__{encode_short(self._get_args())}.pkl"
        remote_path = f"/tmp/task_{self.__class__.__name__}__{encode_short(copy_task._get_args())}.pkl"
        remote_result_path = remote_path + '.result'

        import pickle
        import os

        with open(path, 'wb') as writer:
            pickle.dump(copy_task, writer)

        assert (os.path.getsize(path) > 0)

        # upload serialized payload
        ssh.upload_file(client, path, remote_path)

        command = f"cd {workdir} && {executable} -c \"import pickle as p; task = p.load(open('{remote_path}', 'rb')); r=task.run(); p.dump(r,open('{remote_result_path}', 'wb'))\""
        logger.info("execute command: ", command)

        _, stdout, stderr = client.exec_command(command, get_pty=True, environment=os.environ)

        stderr = stderr.readlines()
        stdout = stdout.readlines()

        if len(stderr) > 0:
            raise Exception(f"failed to execute task remotely. stderr: {stderr}. {stdout}")

        # copy result from remote to local machine
        logger.debug("copy targts from remote to local")

        for target in to_list(self.target()):
            if isinstance(target, LocalTarget):
                local_path = target.path
                # local_path = str(target.path.absolute()) + ".remote"
                logger.info(f"download file {local_path}")
                ssh.download_file(client, target.path, local_path)

        # copy and read result
        local_result_path = remote_result_path + '.local'
        ssh.download_file(client, remote_result_path, local_result_path)

        return pickle.load(open(local_result_path, 'rb'))

    def _create_simple_local_target(self):
        args = self._get_args()
        return LocalTarget(f"/tmp/{self.__class__.__name__}_{encode_short(args)}.output")


class DepTask(Task):
    def run(self):
        pass

    def target(self) -> OutputType:
        return [dep.target() for dep in to_list(self.depends())]


class DownloadTask(Task):
    def __init__(self, url: str, destination: Path, auth: Optional[Tuple[str, str]] = None, headers: Optional[Dict[str, str]] = None) -> None:
        self.url: str = url
        self.destination: Path = Path(destination)
        self.headers = headers
        self.auth = auth

    def run(self):
        download(self.url, str(self.destination.absolute()), auth=self.auth, headers=self.headers)

    def target(self) -> LocalTarget:
        return LocalTarget(self.destination)


class TempDownloadTask(Task):
    def __init__(self, url: str, auth: Optional[Tuple[str, str]] = None, headers: Optional[Dict[str, str]] = None, suffix: Optional[str] = None) -> None:
        self.url: str = url

        filename = str(get_hash(url))
        if suffix:
            filename += suffix

        self.destination: Path = Path("/tmp/", filename).absolute()
        self.headers = headers
        self.auth = auth

    def run(self):
        logger.debug(f"downloading {self.url} to {self.destination}")
        start = default_timer()
        download(self.url, str(self.destination.absolute()), auth=self.auth, headers=self.headers)
        return dict(elapsed=default_timer() - start)

    def target(self) -> LocalTarget:
        return LocalTarget(self.destination)


def get_hash(obj: any) -> str:
    return encode_short(json.dumps(obj))


class BashTask(Task):
    def __init__(self, cmd: List[str]) -> None:
        self.cmd = cmd

    def run(self):
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


class SingleOutputTask(Task):

    def _run(self) -> Union[str, List[str]]:
        pass

    def run(self):
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


class SSHCommandTask(Task):

    def __init__(self, ip: str, username: str, cmd: List[str], pem: Union[str, Path] = None) -> None:
        self.cmd = cmd
        self.pem = pem
        self.ip = ip
        self.username = username

    def run(self):

        logging.info(f"=> executing {self.cmd}")

        client = ssh.get_client(self.ip, self.username, pem=self.pem)
        stdin, stdout, stderr = client.exec_command(" ".join(self.cmd))

        stderr = stderr.readlines()

        if len(stderr) > 0:
            raise Exception(f"failed to execute {self.cmd} - stderr: {stderr}")

        out = stdout.readlines()

        with self.target().open('w') as writer:
            writer.writelines(out)

        return out

    def target(self):
        return self._create_simple_local_target()


class S3UploadTask(Task):

    def __init__(self, local_path: Union[str, Path], target_uri: Union[str, Tuple[str, str]]) -> None:
        self.local_path = Path(local_path)
        self.target_uri = target_uri if isinstance(target_uri, str) else f"s3://{target_uri[0]}/{target_uri[1]}"

    def depends(self) -> Dependency:
        return LocalTarget(self.local_path)

    def run(self):
        assert (self.target().upload(self.local_path))

    def target(self):
        return S3Target.from_uri(self.target_uri)


class S3DownloadTask(Task):

    def __init__(self, uri: Union[str, Tuple[str, str]], local_path: Union[str, Path]) -> None:
        self.local_path = Path(local_path)
        self.uri = uri if isinstance(uri, str) else f"s3://{uri[0]}/{uri[1]}"

    def depends(self):
        return S3Target.from_uri(self.uri)

    def run(self):
        self.depends().download(self.local_path)

    def target(self):
        return LocalTarget(str(self.local_path.absolute()))


class DockerContainerTask(SingleOutputTask):

    def __init__(self, name: str, force: bool = False, raise_exit_code_nonzero: bool = True) -> None:
        self.name = name
        self._container = Container.from_name(name)
        self.raise_exit_code_nonzero = raise_exit_code_nonzero

    def run(self):
        result = self._container.run(raise_exit_code_nonzero=self.raise_exit_code_nonzero)
        return result.json()


def _create_tmp_path(uri: str):
    ext = os.path.splitext(uri)[1]
    if "?" in ext:
        ext = ext.split("?")[0]
    ext = ext or ".output"
    return Path(f"/tmp/{encode_short(uri)}{ext}")


class CopyTask(Task):
    def __init__(self, source: Union[str, Path], destination: Union[str, Path]) -> None:
        self.source = source
        self.destination = destination

    def depends(self):
        return LocalTarget(self.source)

    def run(self):
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


class DownloadAnyTask(DepTask):

    def __init__(self, uri: str, local_path: Union[str, Path] = None, **download_args):
        self.uri = uri
        self.download_args = download_args
        self.local_path = Path(local_path) if local_path else _create_tmp_path(uri)

    def depends(self):
        """ test uri format """
        if self.uri.startswith("s3://"):
            return S3DownloadTask(self.uri, self.local_path, **self.download_args)
        elif self.uri.startswith("http://") or self.uri.startswith("https://"):
            return DownloadTask(self.uri, self.local_path, **self.download_args)
        else:
            return CopyTask(self.uri, self.local_path)

    def target(self):
        return LocalTarget(self.local_path)


class LambdaTarget(Target):

    def __init__(self, lambda_fn: Callable):
        self.lambda_fn = lambda_fn

    def exists(self) -> bool:
        return self.lambda_fn()

    def delete(self):
        pass


class MLDockerTask(DockerContainerTask):

    def __init__(self, container_name: str, inputs: List[str], outputs: List[str], force: bool = False):
        self.inputs = inputs
        self.force = force
        self.container_name = container_name
        self._container = Container.from_name(container_name)

        # get input and output path
        logger.info(self._container.mounts)
        self.input_path = list(filter(lambda m: m.destination == Path('/input'), self._container.mounts))[0].source
        self.output_path = list(filter(lambda m: m.destination == Path('/output'), self._container.mounts))[0].source
        self.outputs = outputs

        # clear mounts
        def clear(dir: str):
            shutil.rmtree(dir, ignore_errors=True)
            os.makedirs(dir, exist_ok=True)

        clear(self.input_path)
        clear(self.output_path)

    def depends(self):
        return [
            DownloadAnyTask(uri, self.input_path / os.path.basename(uri)) for uri in self.inputs
        ] + [
            LambdaTarget(lambda: os.path.exists(self.output_path)),
            LambdaTarget(lambda: os.path.exists(self.input_path)),
        ]

    def run(self):
        return self._container.run(raise_exit_code_nonzero=True)

    def target(self):
        return [LocalTarget(self.output_path / o) for o in self.outputs]

from pathlib import Path
from timeit import default_timer
from .utils import logger  # noqa

try:
    import docker
except:
    logger.warning("docker tasks cannot be used. 'docker' package is missing. Use <pip install docker>")

import enum
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
from typing import Dict, List
import shutil
import os


class ContainerStatus(str, enum.Enum):
    exited = 'exited'
    running = 'running'
    paused = 'paused'
    restarting = 'restarting'
    invalid = 'invalid'
    dead = 'dead'
    created = 'created'


class Image(BaseModel):
    id: str
    name: str


class ContainerState(BaseModel):
    status: ContainerStatus
    exit_code: Optional[int]
    error: Optional[str]
    oom_killed: Optional[bool]
    pid: Optional[str]

    class Config:
        fields = {
            "exit_code": "ExitCode",
            "error": "Error",
            "status": "Status",
            "oom_killed": "OOMKilled",
            "pid": "Pid"
        }


class Mount(BaseModel):
    source: Path
    destination: Path
    mode: str
    rw: bool

    class Config:
        fields = {
            "source": "Source",
            "destination": "Destination",
            "mode": "Mode",
            "rw": "RW"
        }


class LastRun(BaseModel):
    start: datetime
    finish: datetime


class DockerException(Exception):
    pass


class ContainerAlreadyRunningException(DockerException):
    def __init__(self, container: "Container") -> None:
        self.container = container
        super().__init__(f"container {container.name} is already running")


class Result(BaseModel):
    logs: List[str]
    exit_code: int
    elapsed: float


class ContainerExitCodeException(DockerException):
    def __init__(self, container: "Container", exit_code: int,
                 logs: List[str],
                 elapsed: float):
        self.elapsed = elapsed
        self.exit_code = exit_code
        self.container = container
        self.logs = logs
        super().__init__(f"container {container.name} ran for {elapsed:.2f} seconds and has invalid exit code {exit_code}. Logs={logs}")


class ContainerNotFoundException(DockerException):
    pass


def parse_attrs(attrs: dict):

    container_id = attrs['Id']
    docker_image_id = attrs['Image']
    docker_image_name = attrs['Config']['Image']
    env = attrs['Config']['Env']
    state = attrs.get('State', {"Status": "invalid"})

    if state['Status'] == 'exited':
        last_start = state.get("StartedAt", None)
        last_finish = state.get("FinishedAt", None)
    else:
        last_start, last_finish = None, None
    env = dict([e.split("=") for e in env])
    restart_count = attrs.get("RestartCount", 0)
    mounts = [Mount(**m) for m in attrs.get("Mounts", [])]

    return dict(
        created=attrs.get("Created", None),
        restart_count=restart_count,
        env=env,
        id=container_id,
        state=ContainerState(
            **state
        ),
        image=Image(
            id=docker_image_id,
            name=docker_image_name
        ),
        last=None if last_start is None else LastRun(start=last_start, finish=last_finish),
        mounts=mounts
    )


class Container(BaseModel):
    name: str
    id: str
    created: datetime

    image: Image
    last: Optional[LastRun]

    state: ContainerState
    env: Dict[str, str]
    mounts: List[Mount]
    restart_count: int

    @classmethod
    def from_name(cls, name: str):
        obj = Container.obj(name)
        if obj is None:
            raise ContainerNotFoundException(f"container {name} cannot be found")

        attrs = Container.obj(name).attrs

        return cls(
            name=name,
            **parse_attrs(attrs)
        )

    @staticmethod
    def obj(name: str):
        return docker.from_env().containers.get(name)

    def get(self):
        return Container.obj(self.name)

    @property
    def attrs(self):
        return self.get().attrs

    def update(self):
        """ updates the attributes of this container """
        for k, v in parse_attrs(self.attrs).items():
            setattr(self, k, v)

    def run(self, raise_exit_code_nonzero: bool = False):
        self.update()

        if self.state.status == ContainerStatus.running:
            raise ContainerAlreadyRunningException(self)

        container = self.get()
        start_time = default_timer()

        logger.info(f'starting container {self.name}')
        container.start()

        logger.info(f"waiting for results for container {self.name}")
        result = container.wait()
        logger.info(f"container {self.name} finished: {result}")

        elapsed = default_timer() - start_time
        exit_code = result['StatusCode']

        logger.info(f"container {self.name} exit code: {exit_code}")

        logs = container.logs()
        logs = str(logs, 'utf-8')
        log_lines = logs.split('\n')

        if raise_exit_code_nonzero and exit_code != 0:
            raise ContainerExitCodeException(
                container=self,
                exit_code=exit_code,
                logs=log_lines,
                elapsed=elapsed
            )

        return Result(logs=log_lines, exit_code=exit_code, elapsed=elapsed)

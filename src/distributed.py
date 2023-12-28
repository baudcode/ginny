from .base import Task
from .utils import import_task
from .schedule import run


from pathlib import Path
from typing import List
import time
import redis
import uuid
import json
import inspect
import sys
from typing import Literal
from contextlib import redirect_stdout
import traceback
import io
from unittest.mock import MagicMock
from typing import Union
from datetime import datetime
"""
Each tasks gets serialized into
- module to execute
- input json data 

- docker container that gets deployed via docker swarm on any instance that reads and writes from redis


What we need?
- Environment
- Code to copy
- Requirements
- capture stdout?
"""


def redirect_mock(_):
    yield MagicMock()


def fullname(klass):
    module = klass.__module__
    if module == 'builtins':
        return klass.__qualname__  # avoid outputs like 'builtins.str'
    return module + '.' + klass.__qualname__


def data2task(data):
    task_module = data['module']
    task_args = data['data']

    print(f"loading task {task_module} | args: {task_args}")

    TaskClass = import_task(task_module)

    print(f"restored task {TaskClass}")
    task: Task = TaskClass(**task_args)
    return task


class CustomEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, Path):
            return str(obj.absolute())

        return json.JSONEncoder.default(self, obj)


class RedisTaskManager:
    def __init__(self, host: str, queue_name: str, port=6379, ttl=1500):
        self.client = redis.StrictRedis(host=host, port=port, db=0)
        self._name = queue_name
        self._tasks_name = queue_name + "_tasks"
        self._states = queue_name + "_states"
        self._result_name = queue_name + "_results"
        self._log_name = queue_name + "_logs"
        self.ttl = ttl

    def reset(self):
        self.client.delete(self._tasks_name, self._states, self._result_name, self._name)

    def put(self, task: Task, data: dict):
        task_id = str(uuid.uuid4())

        # clsmembers = inspect.getmembers(sys.modules[__name__], inspect.isclass)
        module = fullname(task)
        print(f"storing task {task_id} as {module}")

        self.client.hset(self._tasks_name, key=task_id, value=json.dumps(
            dict(module=module, data=data), cls=CustomEncoder
        ))

        print(f"add task {task_id} to queue")
        self.client.rpush(self._name, task_id)

        self.client.hset(self._states, task_id, "init")
        return task_id

    def pull(self):
        """ server needs to have Ack for receiving the message
        needs to make the message available after retention period if message was not processed
        """
        task_id = self.client.lpop(self._name)
        if task_id is None:
            return None

        # TODO: add task to processing queue
        self.client.hset(self._states, task_id, "processing")

        print(f"got next task {task_id}")
        task_serialized = self.client.hget(self._tasks_name, task_id)
        queue_task_data = json.loads(task_serialized)

        try:
            task = data2task(queue_task_data)
        except Exception as e:
            print("Exception: ", traceback.format_exc())
            task = None

        return task, task_id

    def get_tasks_by_state(self, state: Literal['init', 'processing', 'failure', 'success']):
        return list(filter(lambda x: str(self.client.hget(self._states, x), "utf-8") == state, self.client.hkeys(self._states)))

    def summary(self):
        for state in ['init', 'processing', 'failure', 'success']:
            print(f"State {state} => {self.get_tasks_by_state(state)}")

    def set_result(self, task_id: str, data: dict, successful: bool):
        # remove from processing queue
        # put result in
        self.client.hset(self._result_name, task_id, json.dumps(data))
        if successful:
            self.client.hset(self._states, task_id, "success")
        else:
            self.client.hset(self._states, task_id, "failure")

    def get_status(self, task_id: str) -> Literal['init', 'processing', 'success', 'failure']:
        status = self.client.hget(self._states, task_id)
        if status is None:
            return None
        return str(status, "utf-8")

    def get_result(self, task_id: str):
        result = self.client.hget(self._result_name, task_id)
        if result is None:
            return None

        return json.loads(result)

    def get_logs(self, task_id: str):
        logs = self.client.hget(self._log_name, task_id)
        if logs is None:
            return None

        return str(logs, "utf-8")

    def set_logs(self, task_id: str, data: str):
        self.client.hset(self._log_name, task_id, data)

    def run(self, timeout=5):
        while True:

            task_data = self.pull()

            print("fetch task data: ", task_data)
            if task_data is not None:
                task, task_id = task_data
                print(f"running task {task}")
                if task is None:
                    print(f"warning: skipping task {task}")
                    continue

                try:
                    results = run(task)
                    print("results: ", results)

                    self.set_result(task_id, results.get(task), successful=True)
                    task._logger.info(f"Finished task {task_id}")
                except Exception as e:
                    self.set_result(task_id, dict(tb=traceback.format_exc(), exception=e.__class__.__name__), successful=False)
                    task._logger.exception(e)

                # publish logs
                queue.set_logs(task_id, "testing")

            time.sleep(timeout)

            # print("next iteration...", f.readlines())
            # print(self.summary())


def run_distributed(task: Union[Task, List[Task]], manager: RedisTaskManager, timeout=.5):

    started_at = datetime.utcnow()
    tasks = task if isinstance(task, list) else [task]
    task_ids = set()

    for task in tasks:
        task_id = manager.put(task.__class__, task._items)
        task_ids.add(task_id)

    visited = set()

    while any(manager.get_status(task_id) not in ['success', 'failure'] for task_id in task_ids):
        for task_id in task_ids.difference(visited):

            status = manager.get_status(task_id)
            print("task_id: ", task_id, "status: ", status)

            if status not in ['failure', 'success']:
                continue

            visited.add(task_id)
            elapsed = (datetime.utcnow() - started_at).total_seconds()

            if status == 'failure':
                logs = manager.get_logs(task_id)
                print(f"=> {task_id} [{status}]: {logs} [elasped={elapsed:.2f}]")
            else:
                print(f"=> {task_id} [{status}] [elasped={elapsed:.2f}]")

        time.sleep(timeout)

    return [manager.get_result(task_id) for task_id in task_ids]


if __name__ == "__main__":
    # from . import DownloadTask
    queue = RedisTaskManager("localhost", "test")
    # queue.reset()
    queue.run()

    # task_id = queue.put(DownloadTask, dict(
    #     url="https://static.wikia.nocookie.net/harrypotter/images/e/e9/Ginny-HQ-ginevra-ginny-weasley.jpg/revision/latest/scale-to-width-down/250?cb=20150228082608&path-prefix=de",
    #     destination='image.jpg'
    # ))

    # print(queue)
    # print(queue.summary())
    # next_task = queue.pull()
    # print(queue.summary())
    # queue.set_result(task_id, dict(uri="s3://ai-datastore/..."), successful=True)
    # print(queue.summary())
    # queue.set_result(task_id, dict(uri="s3://ai-datastore/..."), successful=False)
    # print(queue.summary())
    # print(queue.get_result(task_id))
    # run_distributed(queue)

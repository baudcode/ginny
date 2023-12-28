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
        quuee_task_data = json.loads(task_serialized)

        task_module = quuee_task_data['module']
        task_args = quuee_task_data['data']

        print(f"loading task {task_module} | args: {task_args}")

        TaskClass = import_task(task_module)

        print(f"restored task {TaskClass}")
        task: Task = TaskClass(**task_args)

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
        return self.client.hget(self._states, task_id)

    def get_result(self, task_id: str):
        result = self.client.hget(self._result_name, task_id)
        if result is None:
            return None

        return json.loads(result)

    def run(self, timeout=5):
        while True:
            # f = io.StringIO()
            # with redirect_stdout(f):
            task_data = self.pull()
            print("fetch task data: ", task_data)
            if task_data is not None:
                task, task_id = task_data
                print(f"running task {task}")
                try:
                    results = run(task)
                    self.set_result(task_id, results, successful=True)
                except Exception as e:
                    self.set_result(task_id, dict(tb=traceback.format_exc(), exception=e.__class__.__name__), successful=False)

            time.sleep(timeout)

            # print("next iteration...", f.readlines())
            # print(self.summary())


def run_distributed(task: Union[Task, List[Task]], manager: RedisTaskManager, timeout=.5):

    tasks = task if isinstance(task, list) else [task]
    task_ids = []

    for task in tasks:
        task_id = manager.put(task.__class__, task._items)
        task_ids.append(task_id)

    while any(manager.get_status(task_id) not in ['success', 'failure'] for task_id in task_ids):
        print(f"=> {task_ids} -> {[manager.get_status(task_id) for task_id in task_ids]}")
        time.sleep(timeout)

    return [manager.get_result(task_id) for task_id in task_ids]


if __name__ == "__main__":
    # from . import DownloadTask
    queue = RedisTaskManager("localhost", "test")
    queue.reset()
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

import dataclasses
from typing import List

from src import DepTask, GlobalVar, LocalTarget, Task
from src.base import DynamicTask, IterableParameter, IterableParameterMap


@dataclasses.dataclass(frozen=True)
class A(Task):
    order_id: str = GlobalVar("order_id")
    pano_id: str

    def run(self, *args, **kwargs):
        self.target().write_text("hello")

    def target(self):
        return LocalTarget("/tmp/a.txt")

@dataclasses.dataclass(frozen=True)
class B(Task):
    def run(self, *args, **kwargs):
        self.target().write_text("world")

    def target(self):
        return LocalTarget("/tmp/b.txt")

@dataclasses.dataclass(frozen=True)
class GenerateParameters(Task):
    def run(self, *args, **kwargs):
        results = [
            {"pano_id": "testing123", "order_id": "1"},
            {"pano_id": "testing456", "order_id": "2"},
            {"pano_id": "testing4567", "order_id": "3"}
        ]
        self.target().set(results)
        return results

    def target(self):
        return IterableParameterMap(name='data', keys=['pano_id', 'order_id'])

@dataclasses.dataclass(frozen=True)
class ProcessLine(Task):
    # this task does not have a depends() method

    pano_id: str
    order_id: str

    def run(self, *args, **kwargs):
        self.target().write_text(f"processed {self.order_id} {self.pano_id}")
    
    def target(self):
        return LocalTarget(f"/tmp/processed_{self.order_id}.txt")



@dataclasses.dataclass(frozen=True)
class D(Task):
    # this task does not have a depends() method

    def run(self, *args, **kwargs):
        print("do some stuff D")
        self.target().write_text("do some stuff D")
    
    def target(self):
        return LocalTarget(f"/tmp/processed_D.txt")



class ProcessLines(DynamicTask):

    @property
    def taskclass(self):
        return ProcessLine

    def depends(self):
        return [GenerateParameters(), D()]


@dataclasses.dataclass(frozen=True)
class DynamicPipeline(Task):
    order_id: str = GlobalVar("order_id")

    def depends(self) -> List[Task]:
        a = A(order_id=self.order_id, pano_id="testing123")
        b = B()
        c = ProcessLines()
        return [a, b, c]

    def run(self, *args, **kwargs):
        print("Running pipeline")
        data1 = self.depends()[0].target().read_text()
        print("Task A exists: ", self.depends()[0].target().exists())
        print("Task A result: ", data1)
        data2 = self.depends()[1].target().read_text()
        print("Task B exists: ", self.depends()[1].target().exists())
        print("Task B result: ", data2)
        print("Total result: ")

        result_tasks = self.depends()[2].target()
        dynamic_task_result = "_" + "_".join([t.target().read_text() for t in result_tasks])
        print(dynamic_task_result)
        return data1 + data2 + dynamic_task_result



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

        return data1 + data2

def test_simple_task():
    from src import run

    run(A(pano_id="testing123", order_id="1"))
    run(B())

    gen_params_task = GenerateParameters()
    _ = run(gen_params_task, debug=True)
    assert gen_params_task.target().keys == ['pano_id', 'order_id']


def test_run_pipeline():
    from src import run
    task = Pipeline(order_id="1")
    tasks = run(task, force=True)
    assert tasks[task] == "helloworld"

def test_run_dynamic_task():
    from src import run

    task = ProcessLines()
    results = run(task, force=True)
    print(results)
    print(len(results[task]) == 3)

    pipeline = DynamicPipeline(order_id="1")
    tasks = run(pipeline, force=True)
    assert tasks[pipeline] == "helloworld_processed 1 testing123_processed 2 testing456_processed 3 testing4567"
    
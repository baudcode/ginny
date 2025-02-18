import dataclasses
from typing import List

from src import DepTask, GlobalVar, LocalTarget, Task
from src.base import DynamicTask, IterableParameter, IterableParameterMap


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

@dataclasses.dataclass(frozen=True)
class GenerateParameters(Task):
    def run(self, *args, **kwargs):
        self.target()[2].set([
            {"pano_id": "testing123", "order_id": "1"},
            {"pano_id": "testing456", "order_id": "2"},
            {"pano_id": "testing4567", "order_id": "3"},
        ])

    def target(self):
        return [IterableParameterMap(name='data', keys=['pano_id', 'order_id'])]

@dataclasses.dataclass(frozen=True)
class ProcessLine(Task):
    pano_id: str
    order_id: str

    def run(self, *args, **kwargs):
        self.target().write_text(f"processed {self.order_id} {self.pano_id}")
    
    def target(self):
        return LocalTarget(f"/tmp/processed_{self.order_id}.txt")
    

class ProcessLines(DynamicTask):

    @property
    def taskclass(self):
        return ProcessLine
    
    @property
    def parameter(self):
        return [IterableParameterMap(name='data', keys=['pano_id', 'order_id'])]

    def depends(self):
        return [GenerateParameters()]


@dataclasses.dataclass(frozen=True)
class Pipeline(Task):
    order_id: str = GlobalVar("order_id")

    def depends(self) -> List[Task]:
        a = A(order_id=self.order_id, pano_id="testing123")
        b = B()
        # print_lines = ProcessLines()
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

        # result_tasks = self.depends()[2].target()
        # print([t.target().read_text() for t in result_tasks])
        print(data1 + data2)
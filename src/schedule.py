from typing import Dict
import networkx as nx
from .base import Task, to_list, Target
from typing import List
from multiprocessing import Pool as ProcessPool
from multiprocessing.pool import ThreadPool
from datetime import datetime


def _schedule(task: Task, g: nx.Graph, force: bool = False):

    if not task.done() or force:
        g.add_node(task)

        for dep_task in to_list(task.depends()):

            if isinstance(dep_task, Task):
                g.add_edge(task, dep_task)
                g = _schedule(dep_task, g, force=force)

            elif isinstance(dep_task, Target):
                if not dep_task.exists():
                    raise UnresolvedDependencyException(task, unresolved=dep_task)
            else:
                raise Exception(f"invalid task/target type: {dep_task.__class__.__name__}")

    return g


def schedule(task: Task, force: bool = False) -> nx.Graph:
    return _schedule(task, nx.Graph(), force=force)


def get_path_lengths(root: Task, g: nx.Graph):
    for node in g.nodes:
        yield node, len(nx.shortest_path(g, root, node))


def create_execution_order(root: Task, g: nx.Graph) -> List[List[Task]]:
    # put task into bins
    levels: List[List[Task]] = []

    current_degree: int = 1

    stage: List[Task] = []

    for task, degree in sorted(list(get_path_lengths(root, g)), key=lambda x: x[1]):
        # print(f"adding task {task} to level={degree}")
        if degree != current_degree:
            levels.append(stage)
            stage = [task]
            current_degree = degree
        else:
            stage.append(task)

    levels.append(stage)
    levels = list(filter(lambda x: len(x) != 0, levels))
    return list(reversed(levels))


class UnresolvedDependencyException(Exception):
    def __init__(self, task: Task, unresolved: any = None) -> None:
        self.task = task
        self.unresolved = unresolved
        super().__init__(f"[{task}] has unfullfilled dependency {unresolved if unresolved else list(task.unresolved_dependencies())} for task {task}")


class NoResultException(Exception):
    def __init__(self, task: Task, target: Target) -> None:
        self.task = task
        super().__init__(f"[{task}] has not procuced target {target}")


def run(task: Task, PoolClass=ThreadPool, workers: int = 4, debug: bool = False, force: bool = False) -> Dict[Task, any]:
    g = schedule(task, force=force)
    order = create_execution_order(task, g)

    if debug:
        print("\n\n========= execution order =========")
        for level, tasks in enumerate(order):
            print(f"[level={level}] => ", tasks)
        print("===================================\n\n")

    start = datetime.now()
    print(f'start => {start}')

    all_results = {}

    with PoolClass(workers) as pool:
        for tasks in order:
            print(".")
            scheduled = []
            for task in tasks:
                print(f"=> Running {task}")

                if task.runnable():
                    scheduled.append(task)
                else:
                    raise UnresolvedDependencyException(task)

            results = pool.map(lambda x: x.run(), scheduled)

            # check that all tasks have produced results
            for task in scheduled:
                for target in to_list(task.target()):
                    if not target.exists():
                        raise NoResultException(task, target=target)

            for task, result in zip(scheduled, results):
                all_results[task] = result

    print(f"end => {datetime.now()} | elapsed: {(datetime.now() - start).total_seconds():.2f} seconds")
    return all_results

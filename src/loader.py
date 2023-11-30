import json
import ast
from .base import Task
from .schedule import run

from argparse import ArgumentParser
import argparse
import schedule
import time


def import_task(name: str):
    components = name.split('.')
    mod = __import__(components[0])
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod


weekdays = ['monday', 'tuesday', 'wednesday', 'tursday', 'friday', 'saturday', 'sunday']
units = ['hour', 'minute', 'second', 'day', 'month', 'year']


def main():
    p = ArgumentParser()
    p.add_argument('--task', required=True)
    p.add_argument('--every', choices=weekdays + units + ["one_time"], default='one_time')
    p.add_argument('--count', type=int, default=None)
    p.add_argument('--to', type=int, default=None)
    p.add_argument('--at', help='time of the day, if not set, executed at 0:00')
    p.add_argument('args', nargs=argparse.REMAINDER)
    args = p.parse_args()

    task_class = args.task

    def parse_arg(x: str):

        for f in [ast.literal_eval, json.loads]:
            try:
                t = f(x)
                if isinstance(t, str):
                    raise TypeError('invalid type')
            except:
                pass

        return x

    task_args = list(map(parse_arg, args.args))
    assert (len(task_args) % 2 == 0)

    items = [(
        task_args[i], task_args[i + 1]
    ) for i in range(0, len(task_args), 2)]

    task_args = dict(items)

    TaskClass = import_task(task_class)

    print(f"restored task {TaskClass}")
    task: Task = TaskClass(**task_args)

    if args.every == 'one_time':
        results = run(task)
        print(f"results: {results}")
    else:
        sched = schedule
        if args.count is not None:
            sched = sched.every(args.count)
        else:
            sched = sched.every()

        if args.to is not None:
            sched = sched.to(args.to)

        sched = getattr(sched, args.every + "s")
        if args.at:
            sched = sched.at(args.at)

        sched.do(lambda: run(task))
        print("jobs: ", schedule.get_jobs())

        while True:

            # Checks whether a scheduled task
            # is pending to run or not
            schedule.run_pending()
            time.sleep(1)


if __name__ == "__main__":
    main()

# create visualizations around the graph and its dependecies and output data
import argparse

import networkx as nx

from .base import Task, _get_args
from .schedule import create_execution_order, schedule


def visualize_graph(g: nx.Graph, font_color='red', node_color='black', node_size=100):
    import matplotlib.pyplot as plt
    nx.draw(g, with_labels=True, font_color=font_color, node_color=node_color, node_size=node_size)
    plt.show()

def visualize_graph_using_graphviz(g):
    from graphviz import Digraph

    dot = Digraph()
    # create from networkx graph
    for node in g.nodes:
        dot.node(str(node))
    for edge in g.edges:
        dot.edge(str(edge[0]), str(edge[1]))

    # Render the graph
    dot.render( format='png', view=True)


def print_execution_order(root: str, g: nx.Graph):
    levels = create_execution_order(root, g)
    print(f"Execution order of {root}")
    for i, level in enumerate(levels):
        print(f"-> Level {i}")
        for task in level:
            print(f"  {task}")
        print("\n")

def create_parameter_dict(task):
    return {k: v for k, v in task.__ann.items() if not k.startswith("_")}

def main():
    parser = argparse.ArgumentParser(description='Visualize a graph')
    parser.add_argument("--task", required=True, type=str, help="The task module to visualize (e.g. tests.test_tasks.DynamicPipeline)")
    args = parser.parse_args()

    task_name = args.task.split(".")[-1]
    module_name = ".".join(args.task.split(".")[:-1])
    module = __import__(module_name, fromlist=[task_name])
    task = getattr(module, task_name)
    task_args = _get_args(task)
    print("args: ", task_args)

    parameterized_task: Task = task(**{k: "{{" + f"inputs.parameters.{k}" + "}}" for k, _ in task_args.items()})

    g = schedule(parameterized_task)
    print_execution_order(parameterized_task, g)
    visualize_graph_using_graphviz(g)

if __name__ == "__main__":
    main()


from src.argo import schedule_to_workflow
from tests.test_tasks import Pipeline


def argo_test():

    task = Pipeline()
    workflow = schedule_to_workflow(task, "a-b-process-test", base_image="baudcode/ginny_test:latest")
    workflow.save("test_workflow.yaml")

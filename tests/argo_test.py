
from src.argo import ArgoConfig, schedule_to_workflow
from tests.test_tasks import Pipeline


def test_create_workflow():

    config = ArgoConfig.from_yaml("argo_config.yaml")
    task = Pipeline()
    workflow = schedule_to_workflow(task, "a-b-process-test", config, base_image="baudcode/ginny_test:latest")
    workflow.save("test_workflow.yaml")

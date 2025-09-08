from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

from datetime import datetime

with DAG(
    "hello_kube",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    task = KubernetesPodOperator(
        namespace="default",
        image="busybox",
        cmds=["echo"],
        arguments=["hello from k3s"],
        labels={"test": "hello"},
        name="hello-kube",
        task_id="hello_kube_task",
        get_logs=True
    )

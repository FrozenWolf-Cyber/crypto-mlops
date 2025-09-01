from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_hello():
    print("HELLO FROM HELLO_DAG.PY ✅")

print("📢 Loading hello_dag.py into Airflow...")  # will show in scheduler logs


with DAG(
    dag_id="hello_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # bundles require `schedule` (not schedule_interval)
    catchup=False,
    tags=["example"],
) as dag:
    task = PythonOperator(
        task_id="say_hello",
        python_callable=print_hello,
    )


import sys
from pathlib import Path

# ---------------------------
# Optional: for local testing
# ---------------------------
# Add project root to sys.path so utils.utils can be imported
project_root = Path(__file__).resolve().parents[1]  # mlops/
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))




import os
import sys

def airflow_execution_info():
    print("===== AIRFLOW EXECUTION INFO =====")
    print(f"__file__          : {__file__}")          # Path to this DAG file
    print(f"__name__          : {__name__}")          # Module name
    print(f"Current working dir: {os.getcwd()}")     # cwd at execution
    print(f"sys.path:")                              
    for p in sys.path:                              
        print(f"  {p}")
    print("=================================")

airflow_execution_info()

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import sys
import os
from utils...database.status_db import status_db
from airflow.utils.timezone import datetime
from airflow.models import TaskInstance

def log_start(context):
    ti: TaskInstance = context['ti']
    task_id = ti.task_id
    model_name = "N/A"
    if task_id.startswith("train_"):
        model_name = task_id.replace("train_", "")
    status_db.log_event(
        dag_name=ti.dag_id,
        task_name=ti.task_id,
        model_name=model_name,  # optional
        run_id=ti.run_id,
        event_type="START",
        status="RUNNING",
        message="Task started."
    )

def log_success(context):
    ti: TaskInstance = context['ti']
    task_id = ti.task_id
    model_name = "N/A"
    if task_id.startswith("train_"):
        model_name = task_id.replace("train_", "")
    status_db.log_event(
        dag_name=ti.dag_id,
        task_name=ti.task_id,
        model_name=model_name,  # optional
        run_id=ti.run_id,
        event_type="COMPLETE",
        status="SUCCESS",
        message="Task completed successfully."
    )

def log_failure(context):
    ti: TaskInstance = context['ti']
    task_id = ti.task_id
    model_name = "N/A"
    if task_id.startswith("train_"):
        model_name = task_id.replace("train_", "")
    status_db.log_event(
        dag_name=ti.dag_id,
        task_name=ti.task_id,
        model_name=model_name,  # optional
        run_id=ti.run_id,
        event_type="COMPLETE",
        status="FAILED",
        message=str(context.get("exception"))
    )

from airflow.utils.timezone import datetime  # keep this import

def cleanup():
    status_db.cleanup_old_events(days=365)

with DAG(
    "cleanup_old_events",
    schedule="0 0 * * *",  # daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    PythonOperator(
        task_id="cleanup_old_events",
        python_callable=cleanup,
                on_execute_callback=log_start,
                on_success_callback=log_success,
                on_failure_callback=log_failure,
    )

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
from airflow.providers.standard.operators.bash import BashOperator
import sys
import os
from utils.database.status_db import status_db
from airflow.utils.timezone import datetime
from airflow.models import TaskInstance

start_date = datetime(2025, 9, 1) 

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


# =========================
# DAG 2: TRL Inference Pipeline
# =========================
with DAG(
    'trl_inference_pipeline',
    schedule='*/30 * * * *',  # Every 30 mins
    start_date=start_date,
    catchup=False,
    max_active_runs=1
) as dag2:

    past_news_task = BashOperator(
        task_id="past_news_scrape",
        bash_command="python -m articles_runner.past_news_scrape",
                on_execute_callback=log_start,
                on_success_callback=log_success,
                on_failure_callback=log_failure,
    )

    trl_maker_task = BashOperator(
        task_id="trl_onnx_maker",
        bash_command="python -m serve.trl_onnx_maker",
                on_execute_callback=log_start,
                on_success_callback=log_success,
                on_failure_callback=log_failure,
    )

    trl_inference_task = BashOperator(
        task_id="trl_inference",
        bash_command="python -m serve.trl_inference",
                on_execute_callback=log_start,
                on_success_callback=log_success,
                on_failure_callback=log_failure,
    )


    past_news_task >> trl_maker_task >> trl_inference_task

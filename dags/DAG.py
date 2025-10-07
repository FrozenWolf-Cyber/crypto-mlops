### DAG for entire pipeline orchestration


### dag 1-> trigger this dag once every 3 days and start from now 
### call pre-train.py
### from aiflow_db.py -> flush all and init_entries states
### call vast_ai_train.py -> creates crypto_1_model_1, crypto_1_model_2, crypto_2_model_1, crypto_2_model_2, crypto_3_model_1, crypto_3_model_2, trl_model
		## model_1 -> lightgbm, model_2 -> tst
  
### for each of those 3*2 + 1 models -> monitor state using airflow_db.py get_status ## 'PENDING' | 'RUNNING' | 'SUCCESS' | 'FAILED'
### when one of them is 'SUCCESS' -> trigger the following steps for that model
	### on success -> post-train-reconcile.py --crypto crypto_i --model model_j
	### on sucess for trl_model -> post-train-trl.py
### when one of them is 'FAILED skip
### when all of reach 'SUCCESS' or 'FAILED' -> kill all vast_ai instances -> kill_vast_ai_instances.py

# dag 2 -> run this every 30 mins, ### if post-train-trl.py is currently running, wait for it to finish
### call python past_news_scrape.py
### call python trl_onnx_masker.py
### call python trl-inference.py -> creates trl predictions for all test data and saves to psql


### python consumer.py (crypto, model, v1) -> 2x2x3 -> downloads dataset, predictions (trigger initially) from s3 if not present locally
### -> local csv writer 
### -> make sure no duplicates in either of the csv -> skip those while listening
### -> V1 writes on main csv raw data, while V1, V2, V3 write its predictions to separate csvs
### -> infer through fastapi
### -> psql writer (db.py session pool)
### resume all consumers based on available versions -> consumer_control.py

### python fastapi inference.py  -> fastapi_app.py
### prometheus fastapi_instrumentator
### training
### -> create vast ai instance and install k8 and connect to our k8s cluster
### -> slice train data, update test data and upload training data to s3
### -> submit using dag's kube job operator on lightgbm_train.py, tst_train.py, trl_train.py for each crypto [assign max memory needed and handle dynaically during dag]
### -> train model -> (need ENV variables) mlflow versioning staging handled (model manager trigger at end of script) -> also uploads predictions to s3

### on training end of xth model version v:
### stop consumers for x-v-2  -> consumer start.py
### stop consumers for x-v-3  -> consumer start.py
### pull new fastapi models from s3 -> trigger fastapi_app.py /refresh endpoint
### renamed pred of x-v-3 locally to x-v-2 -> need to create a util
### start consumers for x-v-2 -> consumer start.py
### download pred of new x-v-3 from s3 to local as x-v-3 -> s3_manager.py util
### infer remaining test data (delta of what was pushed to s3 before training to till now in raw data) for x-v-3 and save pred locally as x-v-3 -> trigger the fastapi_app.py /predict endpoint util
### push complete pred of x-v-3 to psql -> db.py session pool util
### start consumers for x-v-3 (should be no lag since new data filled in above step) -> consumer start.py
### upload new predictions to s3 (v2 and v3 since new data filled) (concurrently with above step) -> s3_manager.py util


### on crash:
	### upload datasets and predictions to s3 -> s3_manager.py util
 




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
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
import sys
import os
from utils.utils.vast_ai_train import create_instance
from utils.utils.kill_vast_ai_instances import kill_all_vastai_instances
from utils.database.status_db import status_db
from utils.database.airflow_db import db
from datetime import timedelta
import time
from airflow.utils.timezone import datetime

from airflow.models import TaskInstance

from datetime import timedelta

start_date = datetime(2025, 9, 1) 
start_date_earlier = datetime(2023, 1, 1)

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



# Import your scripts
# import pre_train_dataset
# import vast_ai_train
# import post_train_reconcile
# import post_train_trl
# import kill_vast_ai_instances
# import past_news_scrape
# import trl_inference
# import trl_onnx_maker

# =========================
# DAG 1: Training Pipeline
# =========================
def monitor_model_state(model_name, **context):
    time_limit = 5400 # 1.5 hours
    start_time = time.time()
    print(f"Monitoring model: {model_name}")
    if "_" in model_name:
        coin, model = tuple(model_name.split("_"))
    else:
        model = model_name
        coin = "ALL"
        
    while True:
        if time.time() - start_time > time_limit:
            ### If time limit exceeded, return skip_model
            print(f"Time limit exceeded for model {model_name}. Marking as FAILED.")
            db.set_state(model, coin, "FAILED")
            return "skip_model"
        
            #         status -> [
            #     {"model": r[0], "coin": r[1], "state": r[2], "error_message": r[3]}
            #     for r in rows
            # ]
        status = db.get_status()
        print(f"Current status from DB: {status}")
        status = next((item for item in status if item["model"] == model and item["coin"] == coin), None)
        print(f"Monitoring {model_name}, found status entry: {status}")
        status = status["state"]
        if status == "SUCCESS":
            return f"post_train_{model_name}"
        elif status == "FAILED":
            return "skip_model"
        else:
            print(f"Model {model_name} status: {status}. Checking again in 10 seconds...")
            time.sleep(10)  # Wait for 10 seconds before checking again
            continue

def monitor_all_state_to_kill(**context):
    time_limit = 120*60 # 2 hours
    start_time = time.time()
    print(f"Monitoring all models to kill instances")
        
    while True:
        if time.time() - start_time > time_limit:
            ### If time limit exceeded, return skip_model
            print(f"Time limit exceeded for monitoring all models. Proceeding to kill instances.")
            kill_all_vastai_instances()
            return "kill_vast_ai_instances"
        
            #         status -> [
            #     {"model": r[0], "coin": r[1], "state": r[2], "error_message": r[3]}
            #     for r in rows
            # ]
        status = db.get_status()
        print(f"Current status from DB: {status}")
        all_done = all(item["state"] in ["SUCCESS", "FAILED"] for item in status)
        if all_done:
            print("All models have reached SUCCESS or FAILED state. Proceeding to kill instances.")
            kill_all_vastai_instances()
            return "kill_vast_ai_instances"
        else:
            print(f"Not all models are done yet. Checking again in 10 seconds...")
            time.sleep(10)  # Wait for 10 seconds before checking again
            continue

def cleanup_on_timeout(context):
    kill_all_vastai_instances()



def create_dag1():
    with DAG(
        'training_pipeline',
        schedule='0 0 */5 * *',  # Every 5 days
        start_date=start_date,
        catchup=False,
        max_active_tasks=10,
        max_active_runs=1,
        dagrun_timeout=timedelta(hours=5),
        on_failure_callback=cleanup_on_timeout,
        on_success_callback=cleanup_on_timeout,
    ) as dag:

        start_pretrain = BashOperator(
            task_id='pre_train_dataset',
            bash_command='PYTHONPATH=..:$PYTHONPATH python -m utils.utils.pre_train_dataset',
                on_execute_callback=log_start,
                on_success_callback=log_success,
                on_failure_callback=log_failure,
        )


        def flush_and_init_callable():
            db.flush()
            db.init_entries()

        flush_and_init = PythonOperator(
            task_id='flush_and_init',
            python_callable=flush_and_init_callable,
                on_execute_callback=log_start,
                on_success_callback=log_success,
                on_failure_callback=log_failure,
        )

        # train_models = BashOperator(
        #     task_id='vast_ai_train',
        #     bash_command='PYTHONPATH=..:$PYTHONPATH python -m utils.utils.vast_ai_train',
        #         on_execute_callback=log_start,
        #         on_success_callback=log_success,
        #         on_failure_callback=log_failure,
        # )
        
        train_models = PythonOperator(
            task_id='vast_ai_train',
            python_callable=create_instance,
                on_execute_callback=log_start,
                on_success_callback=log_success,
                on_failure_callback=log_failure,
            
        )

        # List of models created
        # models = [
        #     "crypto_1_model_1", "crypto_1_model_2",
        #     "crypto_2_model_1", "crypto_2_model_2",
        #     "crypto_3_model_1", "crypto_3_model_2",
        #     "trl_model"
        # ]
        cryptos = ["BTCUSDT"]
        models_ = ["lightgbm", "tst"]
        models = ["trl"]
        for crypto in cryptos:
            for model in models_:
                models.append(f"{crypto}_{model}")
            

        monitor_tasks = {}
        # monitor_tasks_vastkill = {}
        post_tasks = {}

        for model in models:
            monitor_tasks[model] = BranchPythonOperator(
                task_id=f"monitor_{model}",
                python_callable=monitor_model_state,
                op_kwargs={"model_name": model},
                retries=0,          # how many times to retry
                retry_delay=timedelta(minutes=1),  # wait between retries
                priority_weight= 10,
            )
            
            # monitor_tasks_vastkill[model] = PythonOperator(
            #     task_id=f"monitor_vastkill_line_{model}",
            #     python_callable=monitor_model_state,
            #     op_kwargs={"model_name": model},
            #     retries=0,          # how many times to retry
            #     retry_delay=timedelta(minutes=1),  # wait between retries
            # )

            if model != "trl":
                crypto, model_type = model.split("_", 1)
                post_tasks[model] = BashOperator(
                    task_id=f"post_train_{model}",
                    bash_command=f"PYTHONPATH=..:$PYTHONPATH python -m utils.utils.post_train_reconcile --crypto {crypto} --model {model_type}",
                    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
                    retries=0,
                    retry_delay=timedelta(minutes=1),
                on_execute_callback=log_start,
                on_success_callback=log_success,
                on_failure_callback=log_failure,
                )
            else:
                post_tasks[model] = BashOperator(
                    task_id="post_train_trl",
                    bash_command="PYTHONPATH=..:$PYTHONPATH python -m utils.utils.post_train_trl",
                    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
                    retries=0,
                    retry_delay=timedelta(minutes=1),
                on_execute_callback=log_start,
                on_success_callback=log_success,
                on_failure_callback=log_failure,
                )


        skip_task = BashOperator(
            task_id="skip_model",
            bash_command='echo "Model failed, skipping post-training."',
                on_execute_callback=log_start,
                on_success_callback=log_success,
                on_failure_callback=log_failure,
        )

        # kill_instances = PythonOperator(
        #     task_id='kill_vast_ai_instances',
        #     python_callable=kill_all_vastai_instances,
        #         on_execute_callback=log_start,
        #         on_success_callback=log_success,
        #         on_failure_callback=log_failure,
            
        # )
        
        final_kill =   PythonOperator(
            task_id="final_kill_kill_vast_ai_instances",
         python_callable=kill_all_vastai_instances,
                on_execute_callback=log_start,
                on_success_callback=log_success,
                on_failure_callback=log_failure,
            
        )

        monitor_all_state_to_kill_task =PythonOperator(
            priority_weight= 100000,
            task_id='monitor_all_to_kill',
            python_callable=monitor_all_state_to_kill,
                on_execute_callback=log_start,
                on_success_callback=log_success,
                on_failure_callback=log_failure,
            
        )

        start_pretrain >> flush_and_init >> train_models
        train_models >> monitor_all_state_to_kill_task

        for model in models:
            train_models >> monitor_tasks[model]



        # for each monitor, insert a neutral node
        
        for model in models:
            monitor_tasks[model] >> [post_tasks[model], skip_task]
       
        post_tasks_list = list(post_tasks.values())
        for t in post_tasks_list + [skip_task]:
            t >> final_kill


    return dag

dag1 = create_dag1()


def create_dag_initial():
    with DAG(
        'consumer_start',
        schedule='@once',  # Runs exactly once
        start_date=start_date_earlier,
        catchup=False,
        max_active_runs=1
    ) as dag:

        start_pretrain = BashOperator(
            task_id='consumer_start',
            bash_command='PYTHONPATH=..:$PYTHONPATH python -m utils.producer_consumer.consumer_start',
                on_execute_callback=log_start,
                on_success_callback=log_success,
                on_failure_callback=log_failure,
        )
    return dag

dag_initial = create_dag_initial()



def delete_all_process():
    with DAG(
        'consumer_delete', ## never run this dag, only for manual cleanup
        schedule=None,  # Runs never
        start_date=start_date_earlier,
        catchup=False,
        max_active_runs=1
    ) as dag:

        kill = BashOperator(
            task_id='consumer_delete',
            bash_command='PYTHONPATH=..:$PYTHONPATH python -m utils.producer_consumer.kill_all',
                on_execute_callback=log_start,
                on_success_callback=log_success,
                on_failure_callback=log_failure,
        )
    return dag

dag_delete_all_process = delete_all_process()

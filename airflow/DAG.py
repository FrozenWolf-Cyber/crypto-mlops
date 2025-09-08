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
 



from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from ..database.status_db import status_db
from ..database.airflow_db import db
from datetime import timedelta
import time
from airflow.utils.timezone import datetime

from airflow.models import TaskInstance

from datetime import timedelta

start_date = datetime(2025, 9, 1) 

def log_start(context):
    ti: TaskInstance = context['ti']
    status_db.log_event(
        dag_name=ti.dag_id,
        task_name=ti.task_id,
        model_name=ti.xcom_pull(task_ids='get_model_name') or "N/A",  # optional
        run_id=ti.run_id,
        event_type="START",
        status="RUNNING",
        message="Task started."
    )

def log_success(context):
    ti: TaskInstance = context['ti']
    status_db.log_event(
        dag_name=ti.dag_id,
        task_name=ti.task_id,
        model_name="N/A",
        run_id=ti.run_id,
        event_type="COMPLETE",
        status="SUCCESS",
        message="Task completed successfully."
    )

def log_failure(context):
    ti: TaskInstance = context['ti']
    status_db.log_event(
        dag_name=ti.dag_id,
        task_name=ti.task_id,
        model_name="N/A",
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
    status = db.get_status(model_name)
    if status == "SUCCESS":
        return f"post_train_{model_name}"
    elif status == "FAILED":
        return "skip_model"
    else:
        raise ValueError(f"Model {model_name} not ready yet, still {status}")


def create_dag1():
    with DAG(
        'training_pipeline',
        schedule_interval='0 0 */3 * *',  # Every 3 days
        start_date=start_date,
        catchup=False,
        max_active_runs=1
    ) as dag:

        start_pretrain = BashOperator(
            task_id='pre_train_dataset',
            bash_command='python ../utils/pre_train_dataset.py',
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

        train_models = BashOperator(
            task_id='vast_ai_train',
            bash_command='python ../utils/vast_ai_train.py',
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
        cryptos = ["BTCUSDT", "ETHUSDT"]
        models_ = ["lightgbm", "trl"]
        models = ["trl_model"]
        for crypto in cryptos:
            for model in models_:
                models.append(f"{crypto}_{model}")
            

        monitor_tasks = {}
        post_tasks = {}

        for model in models:
            monitor_tasks[model] = BranchPythonOperator(
                task_id=f"monitor_{model}",
                python_callable=monitor_model_state,
                op_kwargs={"model_name": model},
                retries=100,          # how many times to retry
                retry_delay=timedelta(minutes=1),  # wait between retries
                provide_context=True
            )

            if model != "trl_model":
                crypto, model_type = model.split("_", 1)
                post_tasks[model] = BashOperator(
                    task_id=f"post_train_{model}",
                    bash_command=f"python ../utils/post_train_reconcile.py --crypto {crypto} --model {model_type}",
                    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
                    retries=100,
                    retry_delay=timedelta(minutes=1),
                on_execute_callback=log_start,
                on_success_callback=log_success,
                on_failure_callback=log_failure,
                )
            else:
                post_tasks[model] = BashOperator(
                    task_id="post_train_trl",
                    bash_command="python ../utils/post_train_trl.py",
                    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
                    retries=100,
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

        kill_instances = BashOperator(
            task_id="kill_vast_ai_instances",
            bash_command="python ../utils/kill_vast_ai_instances.py",
            trigger_rule=TriggerRule.ALL_DONE,
                on_execute_callback=log_start,
                on_success_callback=log_success,
                on_failure_callback=log_failure,
        )

        # DAG dependencies
        start_pretrain >> flush_and_init >> train_models
        for model in models:
            train_models >> monitor_tasks[model] >> [post_tasks[model], skip_task]
        post_task_list = list(post_tasks.values())
        post_task_list >> kill_instances

    return dag

dag1 = create_dag1()


# =========================
# DAG 2: TRL Inference Pipeline
# =========================
with DAG(
    'trl_inference_pipeline',
    schedule_interval='*/30 * * * *',  # Every 30 mins
    start_date=start_date,
    catchup=False,
    max_active_runs=1
) as dag2:

    past_news_task = BashOperator(
        task_id="past_news_scrape",
        bash_command="python ../articles_runner/past_news_scrape.py",
                on_execute_callback=log_start,
                on_success_callback=log_success,
                on_failure_callback=log_failure,
    )

    trl_maker_task = BashOperator(
        task_id="trl_onnx_maker",
        bash_command="python ../serve/trl_onnx_maker.py",
                on_execute_callback=log_start,
                on_success_callback=log_success,
                on_failure_callback=log_failure,
    )

    trl_inference_task = BashOperator(
        task_id="trl_inference",
        bash_command="python ../serve/trl_inference.py",
                on_execute_callback=log_start,
                on_success_callback=log_success,
                on_failure_callback=log_failure,
    )


    past_news_task >> trl_maker_task >> trl_inference_task


import datetime
def cleanup():
    db.cleanup_old_events(days=365)

with DAG(
    "cleanup_old_events",
    schedule_interval="0 0 * * *",  # daily
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
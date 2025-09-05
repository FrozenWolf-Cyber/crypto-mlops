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
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
import time

# Import your scripts
from scripts import (
    pre_train,
    vast_ai_train,
    post_train_reconcile,
    post_train_trl,
    kill_vast_ai_instances,
    airflow_db,
    past_news_scrape,
    trl_onnx_masker,
    trl_inference
)

# =========================
# DAG 1: Training Pipeline
# =========================
def monitor_model_state(model_name, **context):
    status = airflow_db.get_status(model_name)
    if status == "SUCCESS":
        return f"post_train_{model_name}"
    elif status == "FAILED":
        return "skip_model"
    else:
        # Re-check after delay
        time.sleep(60)
        return "monitor_model_state_task"

def create_dag1():
    with DAG(
        'training_pipeline',
        schedule_interval='0 0 */3 * *',  # Every 3 days
        start_date=days_ago(0),
        catchup=False,
        max_active_runs=1
    ) as dag:

        start_pretrain = PythonOperator(
            task_id='pre_train',
            python_callable=pre_train.run
        )

        flush_and_init = PythonOperator(
            task_id='flush_and_init',
            python_callable=airflow_db.flush_and_init_entries
        )

        train_models = PythonOperator(
            task_id='vast_ai_train',
            python_callable=vast_ai_train.run
        )

        # List of models created
        models = [
            "crypto_1_model_1", "crypto_1_model_2",
            "crypto_2_model_1", "crypto_2_model_2",
            "crypto_3_model_1", "crypto_3_model_2",
            "trl_model"
        ]

        monitor_tasks = {}
        post_tasks = {}

        for model in models:
            monitor_tasks[model] = BranchPythonOperator(
                task_id=f"monitor_{model}",
                python_callable=monitor_model_state,
                op_kwargs={"model_name": model},
                provide_context=True
            )

            if model != "trl_model":
                post_tasks[model] = PythonOperator(
                    task_id=f"post_train_{model}",
                    python_callable=post_train_reconcile.run,
                    op_kwargs={"crypto": model.split("_")[1], "model": model.split("_")[2]},
                    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
                )
            else:
                post_tasks[model] = PythonOperator(
                    task_id=f"post_train_trl",
                    python_callable=post_train_trl.run,
                    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
                )

        skip_task = PythonOperator(
            task_id="skip_model",
            python_callable=lambda: print("Model failed, skipping post-training.")
        )

        kill_instances = PythonOperator(
            task_id="kill_vast_ai_instances",
            python_callable=kill_vast_ai_instances.run,
            trigger_rule=TriggerRule.ALL_DONE
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
    start_date=days_ago(0),
    catchup=False,
    max_active_runs=1
) as dag2:

    past_news_task = PythonOperator(
        task_id='past_news_scrape',
        python_callable=past_news_scrape.run
    )

    trl_masker_task = PythonOperator(
        task_id='trl_onnx_masker',
        python_callable=trl_onnx_masker.run
    )

    trl_inference_task = PythonOperator(
        task_id='trl_inference',
        python_callable=trl_inference.run
    )

    past_news_task >> trl_masker_task >> trl_inference_task


# =========================
# DAG 3: Daily Pre-Train Backup
# =========================
with DAG(
    'daily_pretrain_backup',
    schedule_interval='0 2 * * *',  # Every day at 2AM
    start_date=days_ago(0),
    catchup=False,
    max_active_runs=1
) as dag3:

    daily_pretrain = PythonOperator(
        task_id='pre_train_backup',
        python_callable=pre_train.run
    )

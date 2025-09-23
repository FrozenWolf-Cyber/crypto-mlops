import os
import argparse
import pandas as pd
import lightgbm as lgb
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import mlflow
from .train_utils import load_start_time, preprocess_crypto, download_s3_dataset, convert_to_onnx, log_classification_metrics
from ..artifact_control.model_manager import ModelManager
from ..artifact_control.s3_manager import S3Manager
from ..database.airflow_db import db
import wandb

import time
print("STarting LGB training script...", flush=True)
start_time = load_start_time()
def early_stopping_time(max_time: int, verbose: bool = True):
    """
    LightGBM callback for early stopping based on elapsed wall-clock time.
    """

    start_time = time.time()

    def _callback(env):
        elapsed = time.time() - start_time
        if elapsed > max_time:
            if verbose:
                print(
                    f"⏹️ Early stopping at iteration {env.iteration} "
                    f"after {elapsed:.1f}s (limit={max_time}s)",
                    flush=True,
                )
            # Raise exception to stop training immediately
            raise lgb.callback.EarlyStopException(env.iteration, env.evaluation_result_list)

    _callback.order = 0
    return _callback

def mlflow_lgb_callback(val_data, val_labels, name="val"):
    def _callback(env):
        for data_name, metric_name, value, _ in env.evaluation_result_list:
            mlflow.log_metric(f"{data_name}_{metric_name}", value, step=env.iteration)
            wandb.log({f"{data_name}_{metric_name}": value, "epoch": env.iteration})

        print(f"Iteration {env.iteration}, validation multi_logloss: {env.evaluation_result_list[0][2]}", flush=True)
        if (env.iteration%100 == 0) or (env.iteration == env.end_iteration):
            y_pred_probs = env.model.predict(val_data, num_iteration=env.model.best_iteration)
            y_pred = np.argmax(y_pred_probs, axis=1)

            log_classification_metrics(
                y_pred=y_pred,
                y_true=val_labels,
                name=name,
                step=env.iteration
            )

    _callback.order = 10  # before early stopping
    return _callback


def main(args):
    # ------------------------------
    # Setup
    # ------------------------------
    coin = args.coin
    thresh = args.threshold

    download_s3_dataset(coin, trl_model=False)

    df = pd.read_csv(f"/opt/airflow/custom_persistent_shared/data/prices/{coin}.csv")
    df = df[-args.trainset_size:]
    X_test, y_test = preprocess_crypto(pd.read_csv(f"/opt/airflow/custom_persistent_shared/data/prices/{coin}_test.csv"), horizon=1, threshold=thresh)
    X, y = preprocess_crypto(df, horizon=1, threshold=thresh, balanced=True)

    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=0.05, stratify=y, random_state=42
    )


    
    print(f"Train size: {len(X_train)}, Val size: {len(X_val)}, Test size: {len(X_test)}", flush=True)

    train_data = lgb.Dataset(X_train, label=y_train)
    val_data = lgb.Dataset(X_val, label=y_val)

    params = {
        "objective": "multiclass",
        "num_class": 3,
        "boosting": "gbdt",
        "metric": "multi_logloss",
        "learning_rate": 0.05,
        "num_leaves": 31,
        "verbose": 1,
    }

    mlflow.set_tracking_uri(uri=os.getenv("MLFLOW_URI"))
    mlflow.set_experiment(f"{coin.lower()}-lightgbm")
    mm = ModelManager(os.getenv("MLFLOW_URI"))
    run = wandb.init(project='mlops', entity="frozenwolf", config=vars(args), notes=f"Training Lgb model on {coin}")
    with mlflow.start_run() as run:
        db.set_state("lightgbm", args.coin.upper(), "RUNNING")
        mlflow.log_params(params)

        model = lgb.train(
            params,
            train_data,
            valid_sets=[val_data],
            num_boost_round=args.epochs,
            callbacks=[
                mlflow_lgb_callback(X_val, y_val, name="val"),
                        early_stopping_time(args.max_time)
            ]
        )

        ## get best iteration
        print("Best iteration:", model.best_iteration, flush=True)
        
        # ------------------------------
        # Validation metrics
        # ------------------------------
        y_pred_probs = model.predict(X_val, num_iteration=model.best_iteration)
        y_pred = np.argmax(y_pred_probs, axis=1)

        report_val = log_classification_metrics(
            y_pred=y_pred,
            y_true=y_val,
            name="val",
            step=None
        )
        print("Validation Report:\n", report_val)

        # ------------------------------
        # Test metrics
        # ------------------------------
 
        y_pred_probs = model.predict(X_test, num_iteration=model.best_iteration)
        y_pred = np.argmax(y_pred_probs, axis=1)

        report_test = log_classification_metrics(
            y_pred=y_pred,
            y_true=y_test,
            name="val",
            step=None
        )
        print("Test Report:\n", report_test)
        # ------------------------------
        # Convert to ONNX and save
        # ------------------------------

        print("Saving model...")
        model.save_model("best_model.txt", num_iteration=model.best_iteration)
        model = lgb.Booster(model_file="best_model.txt")
        print("Converting to ONNX...")
        onnx_model = convert_to_onnx(model, type="lightgbm", sample_input=X_train[:1])
        mm.save_model(
            type="lightgbm",
            model=model,
            name=f"{coin.lower()}_lightgbm",
            onnx_model=onnx_model
        )
        mm.enforce_retention(f"{coin.lower()}_lightgbm", max_versions=5, delete_s3=True)
        mm.set_production(f"{coin.lower()}_lightgbm", keep_latest_n=2)

        print("MLflow run completed:", run.info.run_id)
        df = pd.read_csv(f"/opt/airflow/custom_persistent_shared/data/prices/{coin}.csv")
        ### generate predictions for the entire dataset
        X_all, y_all = preprocess_crypto(df, horizon=1, threshold=thresh)
        y_pred_probs = model.predict(X_all, num_iteration=model.best_iteration)
        
        y_pred = np.argmax(y_pred_probs, axis=1)

        report_test = log_classification_metrics(
            y_pred=y_pred,
            y_true=y_all,
            name="past_perf",
            step=None
        )
        print("past_perf:\n", report_test)
        
        y_pred_probs = y_pred_probs.tolist()
        ## add null predictions for the initial rows that were dropped during preprocessing
        df[f"pred"] = y_pred_probs
        df = df[["open_time", "pred"]]
        s3 = S3Manager()
        s3.add_pred_s3(df, coin, 'lightgbm')
        
    db.set_state("lightgbm", args.coin.upper(), "SUCCESS")
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train LightGBM crypto model with MLflow logging")
    parser.add_argument("--coin", type=str, default="BTCUSDT", help="Crypto coin symbol, e.g., BTCUSDT")
    parser.add_argument('--epochs', type=int, default=500, help='Number of training epochs')
    parser.add_argument("--threshold", type=float, default=0.00015, help="Threshold for preprocessing")
    parser.add_argument("--trainset_size", type=float, default=150000, help="Proportion of data to use for training")
    parser.add_argument("--max_time", type=int, default=60*20) ## seconds
    args = parser.parse_args()
    
    try:
        main(args)
    except Exception as e:
        db.set_state("lightgbm", args.coin.upper(), "FAILED", error_message=str(e))
        raise e

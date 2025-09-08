from artifact_control.model_manager import ModelManager
import os
from artifact_control.s3_manager import S3Manager
from transformers.onnx import FeaturesManager
from pathlib import Path
import mlflow
import subprocess
from database.db import crypto_db
import pandas as pd

mlflow.set_tracking_uri(os.getenv("MLFLOW_URI"))
mm = ModelManager(tracking_uri=os.getenv("MLFLOW_URI"))
prod_versions = mm.set_production("trl", keep_latest_n=2)
models = []
prod_versions = sorted(prod_versions, key=lambda v: v.version)
print(f"Production versions for trl: {[v.version for v in prod_versions]}")

from artifact_control.s3_manager import S3Manager
manager = S3Manager()
existing_versions = manager.get_existing_versions("preds", "trl")
print("Existing versions in S3:", existing_versions)

if len(existing_versions) <= 3:
    print("Not enough s3 version to reconcile")
else:
    print("Reassigning s3 preds")
    manager.reassign_pred_s3("preds", "trl")
    crypto_db.shift_predictions("trl", "trl", from_version=3, to_version=2)
    os.remove("/data/predictions/preds/trl/v2.csv")
    os.rename("/data/predictions/preds/trl/v3.csv", "/data/predictions/preds/trl/v2.csv")



manager.download_available_predictions("preds", "trl")
existing_versions = manager.get_existing_versions("preds", "trl")
print("New existing versions in S3:", existing_versions)
version = len(existing_versions)
crypto_db.reset_trl_version(f"{version}")

pred = pd.read_csv(f"/data/predictions/preds/trl/v{version}.csv")
pred['date'] = pd.to_datetime(pred['date']) 
pred = pred.drop_duplicates(subset=['link'])

print("Total new preds", len(pred))

print("Upading new column", version)
crypto_db.upsert_trl_full(pred.copy(), version=version)




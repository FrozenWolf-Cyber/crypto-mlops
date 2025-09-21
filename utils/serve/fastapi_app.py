from fastapi import FastAPI, Body
from model_manager import ModelManager
from prometheus_fastapi_instrumentator import Instrumentator
import mlflow.pyfunc
import logging
import os
import numpy as np
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

app = FastAPI(title="MLflow Inference API")

# Prometheus instrumentation
Instrumentator().instrument(app).expose(app, endpoint="/metrics")

mm = ModelManager(tracking_uri=os.getenv("MLFLOW_URI"))
models = {}  # {name: {version: model_object}}


@app.post("/refresh")
def load_production_models():
    """Load current production models (v1 + last 2) into memory."""
    global models
    models = {}
    log_model = {}
    for coin in ["BTCUSDT"]:
        for name in ["lightgbm", "tst"]:
            name = f"{coin.lower()}_{name}"

            try:
                prod_versions = mm.set_production(name, keep_latest_n=2)
            except Exception as e:
                log.error("Error getting prod for %s: %s", name, e)
                continue
            
            print(f"Production versions for {name}: {[v.version for v in prod_versions]}")
            models[name] = {}
            log_model[name] = []
            for idx, v in enumerate(prod_versions):
                print(f"Loading model {name} version {v.version}, run_id: {v.run_id}")
                models[name][idx] = mm.load_onnx_model(name, v)
                log.info("Loaded model %s version %s as %d", name, v.version, idx + 1)
                log_model[name].append((v.version, idx + 1))
                
    return {"status": "models loaded", "models": log_model}


@app.on_event("startup")
def startup_event():
    print("Starting up and loading models...")
    load_production_models()

@app.post("/is_model_available")
def is_model_available(model_name: str, version: int):
    """Check if a model and version is available."""
    if model_name in models :
        if version in models[model_name]:
            print(f"Model {model_name} version {version} is available.")
            return {"available": True}

    print(f"Model {model_name} version {version} is NOT available.")
    return {"available": False}


@app.post("/predict")
def predict(model_name: str, version: int, features: list = Body(...)):
    """
    features: a list of feature vectors, e.g.
    [[0.1, 0.2, 0.3], [0.5, 0.6, 0.7]]
    """
    ort_session = models[model_name][version]
    batch_features = np.asarray(features, dtype=np.float32)
    # predict batch

    input_name = ort_session.get_inputs()[0].name
    preds = ort_session.run(None, {input_name: batch_features})
    predictions = None
    if 'lightgbm' in model_name:
        preds = preds[1]
        predictions = [list(p.values()) for p in preds]
    else:
        preds = preds[0]
        predictions = preds.tolist()
    
    print(f"Predictions count: {len(predictions)}")
    return {"predictions": predictions}



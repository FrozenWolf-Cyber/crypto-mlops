from ..artifact_control.model_manager import ModelManager
import os
from transformers.onnx import FeaturesManager
from pathlib import Path
import mlflow

mlflow.set_tracking_uri(os.getenv("MLFLOW_URI"))
mm = ModelManager(tracking_uri=os.getenv("MLFLOW_URI"))
prod_versions = mm.set_production("trl", keep_latest_n=2)
models = []
prod_versions = sorted(prod_versions, key=lambda v: v.version)
print(f"Production versions for trl: {[v.version for v in prod_versions]}")

if not os.path.exists("./trl_onnx_models"):
    os.makedirs("./trl_onnx_models")


def create_trl_onnx_model():

    config = {}
    ith = 0
    for idx, v in enumerate(prod_versions):
        ith += 1
        config[ith] = f"./trl_onnx_models/finbert_{v.run_id}.onnx"

        if f"./trl_onnx_models/finbert_{v.run_id}.onnx" in os.listdir("./trl_onnx_models"):
            print(f"ONNX for version {v.version}, run_id: {v.run_id} already exists, skipping export.")
            continue
        
        print(f"Loading version {v.version}, run_id: {v.run_id}")
        (model, tokenizer), _ = mm.load_model("trl", v.version, model_type="trl")
        model = model.merge_and_unload()
        print("Model loaded.")
        feature = "sequence-classification"
        # Infer ONNX export configuration
        model_kind, onnx_config_class = FeaturesManager.check_supported_model_or_raise(
            model, feature=feature
        )
        onnx_config = onnx_config_class(model.config)

        # Perform export
        output_path = Path(f"./trl_onnx_models/finbert_{v.run_id}.onnx")
        print(f"Model {model_kind} tokenizer {tokenizer}")
        from transformers.onnx import export as onnx_export
        onnx_inputs, onnx_outputs =  onnx_export(
            preprocessor=tokenizer,
            model=model,
            config=onnx_config,
            opset=14,
            output=output_path,
        )
        tokenizer.save_pretrained(f"./trl_onnx_models/finbert_tokenizer_{v.run_id}")
        print("Exported to ONNX:", output_path)

    ### delete other versions of model weight and tokenizer
    for f in os.listdir("./trl_onnx_models"):
        if ".json" in f:
            continue
        if all(v.run_id not in f for v in prod_versions):
            print(f"Deleting old file/folder: {f}")

            if os.path.isdir(os.path.join("./trl_onnx_models", f)):
                import shutil
                shutil.rmtree(os.path.join("./trl_onnx_models", f))
            else:
                os.remove(os.path.join("./trl_onnx_models", f))

    import json
    with open("./trl_onnx_models/trl_onnx_config.json", "w") as f:
        json.dump(config, f)
        
        
create_trl_onnx_model()
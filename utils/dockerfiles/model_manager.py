# model_manager.py
from mlflow.tracking import MlflowClient
from urllib.parse import urlparse
import boto3
import logging
import os
import mlflow

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


class ModelManager:
    def __init__(self, tracking_uri=None, aws_region='auto'):
        self.client = MlflowClient(tracking_uri)
        self.s3 = boto3.client(
    service_name="s3",
     endpoint_url=os.getenv("S3_URL"),
     aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
     aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
     region_name="auto"
        )



    def list_versions(self, name):
        """Return all versions sorted by creation time ascending."""
        versions = self.client.search_model_versions(f"name='{name}'")
        versions = sorted(versions, key=lambda v: v.creation_timestamp)
        print(f"Found {len(versions)} versions for model {name}")
        return versions

    def register(self, model_uri, name):
        """Register a new version from run artifact path."""
        mv = self.client.create_model_version(name=name, source=model_uri, run_id=None)
        log.info("Registered %s v%s", name, mv.version)
        return mv

    def enforce_retention(self, name, max_versions=10, delete_s3=True):
        """Keep version 1 and the last (max_versions - 1). Delete older ones."""
        versions = self.list_versions(name)
        if len(versions) <= max_versions:
            return

        print("Initial version stages before pruning:")
        for v in versions:
            log.info("Version %s: stage=%s", v.version, v.current_stage)
        # versions to keep: v1 and last max_versions-1
        keep_versions = ['1']
        for v in versions[-(max_versions - 1):]:
            keep_versions.append(v.version)

        for v in versions:
            if v.version not in keep_versions:
                log.info("Pruning %s --- %s -- %s", name, v.version, v.source)
                path = v.source.split("models:/")[-1]
                print("Model path to delete:", path)
                ## list all folders in s3://mlops/ml-flow/models/
                available = self.s3.list_objects_v2(Bucket="mlops", Prefix="ml-flow/")
                available = [obj['Key'] for obj in available.get('Contents', [])]

                if delete_s3:
                    for obj in available:
                        if path in obj:
                            obj = obj.split('/artifacts/')[0]
                            print("!!!Deleting S3 prefix:", f"s3://mlops/{obj}")
                            self._delete_s3_prefix(f"s3://mlops/{obj}")
                            break
                        
                self.client.delete_model_version(name, str(v.version))
                
        versions = self.list_versions(name)
        print("Final version stages after pruning:")
        for v in versions:
            log.info("Version %s: stage=%s", v.version, v.current_stage)
            
    def set_production(self, name, keep_latest_n=2):
        """Mark v1 and last N as Production, rest as Archived."""
        versions = self.list_versions(name)
        if not versions:
            return []

        versions = sorted(versions, key=lambda v: int(v.version))
        ## print all versions and stages
        print("Current version stages:")
        for v in versions:
            log.info("Version %s: stage=%s", v.version, v.current_stage)

        # production = version 1 + last keep_latest_n
        prod_versions = ['1']
        for v in versions[-keep_latest_n:]:
            prod_versions.append(v.version)

        print(f"Setting production versions for {name}: {prod_versions}")

        result = []
        for v in versions:
            if v.version in prod_versions:
                stage = "Production"
            else:
                stage = "Archived"

            if v.current_stage != stage:
                print(f"Transitioning {name} v{v.version} to {stage}")
                self.client.transition_model_version_stage(
                    name=name,
                    version=str(v.version),
                    stage=stage,
                    archive_existing_versions=False,
                )
            if v.version in prod_versions:
                result.append(v)
                
            print(f"Set {name} production versions: {[v.version for v in result]}")
            
        versions = self.list_versions(name)
        versions = sorted(versions, key=lambda v: int(v.version))
        production_versions = []
        print("Final version stages:")
        for v in versions:
            if v.current_stage == "Production":
                production_versions.append(v)
            log.info("Version %s: stage=%s", v.version, v.current_stage)
            
        return production_versions


    def _delete_s3_prefix(self, s3_uri: str):
        """
        Delete all objects under a given S3 (R2) URI prefix.
        Compatible with Cloudflare R2 (manual list_objects_v2 loop).
        """
        parsed = urlparse(s3_uri, allow_fragments=False)
        bucket = parsed.netloc
        prefix = parsed.path.lstrip("/")  # "2" if s3://mlops/2

        print(f"Deleting from bucket={bucket}, prefix={prefix}")

        continuation_token = None
        while True:
            kwargs = {"Bucket": bucket, "Prefix": prefix}
            if continuation_token:
                kwargs["ContinuationToken"] = continuation_token

            resp = self.s3.list_objects_v2(**kwargs)

            for obj in resp.get("Contents", []):
                key = obj["Key"]
                print("Deleting:", key)
                self.s3.delete_object(Bucket=bucket, Key=key)

            if resp.get("IsTruncated"):
                continuation_token = resp["NextContinuationToken"]
            else:
                break

    def save_model(self, type, model, name, onnx_model=None, tokenizer=None):
        import mlflow
        """
        Save model artifacts during training:
        - Original model (LightGBM / PyTorch / HF)
        - ONNX model (optional)
        - LoRA adapter folder (optional)
        - is_lora: True if only saving LoRA weights
        """
        artifact_path = name
    
        # 1️⃣ Save original model only if not LoRA

        if type.lower() == "lightgbm":
            import mlflow.lightgbm
            mlflow.lightgbm.log_model(model, artifact_path=artifact_path)
        elif type.lower() == "tst":
            import mlflow.pytorch
            mlflow.pytorch.log_model(model, artifact_path=artifact_path)
        elif type.lower() == "trl":
            import mlflow.transformers
            mlflow.transformers.log_model(
                transformers_model={
                    "model": model,
                    "tokenizer": tokenizer,
                },artifact_path=artifact_path)
        else:
            raise ValueError("Unsupported model type: %s" % type)

        # 2️⃣ Save ONNX if provided
        if onnx_model is not None:
            onnx_path = f"{name}_tmp.onnx"
            import onnx
            onnx.save(onnx_model, onnx_path)
            mlflow.log_artifact(onnx_path, artifact_path=artifact_path + "/onnx")
            os.remove(onnx_path)
            log.info("Saved ONNX model as artifact for %s", name)
    
        
        
        result = mlflow.register_model(
            model_uri=f"runs:/{mlflow.active_run().info.run_id}/{artifact_path}",
            name=name
        )
        log.info("Registered %s v%s", name, result.version)
        return result


    def load_model(self, name, version, model_type="pytorch"):
        """
        Load a specific version of a registered model from MLflow.

        Args:
            name (str): registered model name
            version (str or int): specific model version
            model_type (str): "pytorch", "lightgbm", "onnx", or "trl"

        Returns:
            - If model_type="trl": ((model, tokenizer), version)
            - Else: (model object, version)
        """
        import mlflow

        model_uri = f"models:/{name}/{version}"
        print(f"Loading model URI: {model_uri}")

        model_info = mlflow.models.get_model_info(model_uri)
        print("Model info:", list(model_info.flavors.keys()))

        if model_type.lower() == "pytorch":
            import mlflow.pytorch
            model = mlflow.pytorch.load_model(model_uri)
        elif model_type.lower() == "lightgbm":
            import mlflow.lightgbm
            model = mlflow.lightgbm.load_model(model_uri)
        elif model_type.lower() == "onnx":
            import mlflow.onnx, onnx
            path = mlflow.onnx.load_model(model_uri)
            model = onnx.load(path)
        elif model_type.lower() == "trl":
            import mlflow.transformers
            model_dict = mlflow.transformers.load_model(model_uri, return_type='components')
            model = model_dict["model"]
            tokenizer = model_dict["tokenizer"]
            return (model, tokenizer), version
        else:
            raise ValueError(f"Unsupported model_type: {model_type}")

        print(f"Loaded {model_type} model '{name}' version {version}")
        return model, version

    def load_onnx_model(self, name, version):
        """
        Load an ONNX model from a registered MLflow model version.
    
        Args:
            name (str): Registered model name
            version (str or int): Model version to load
    
        Returns:
            onnx.ModelProto: Loaded ONNX model object
        """
        import mlflow
        from onnx import load as onnx_load
        import onnxruntime as ort
    
    

        run_id = version.run_id
        artifact_path = name
        artifacts = self.client.list_artifacts(run_id)
        print(f"Artifacts for run_id {run_id}: {[a.path for a in artifacts]}")
        
        local_path = self.client.download_artifacts(
            run_id, artifact_path
        )
        print(f"Downloaded ONNX model locally to: {local_path}")

        local_path = os.path.join(local_path, f'onnx/{name}_tmp.onnx')

        print(f"ONNX model '{name}' version {version} loaded successfully!")
        ort_session = ort.InferenceSession(local_path)

        return ort_session

    def load_latest_model(self, name, model_type="pytorch"):
        """
        Load the latest version of a registered model from MLflow.

        Args:
            name (str): registered model name
            model_type (str): "pytorch", "lightgbm", "onnx", or "trl"

        Returns:
            - If model_type="trl": ((model, tokenizer), latest_version)
            - Else: (model object, latest_version)
        """
        try:
            versions = self.client.get_latest_versions(name)
        except Exception as e:
            print(f"Error fetching versions for model {name}: {e}")
            return None, None

        if not versions:
            return None, None

        latest_version = sorted(versions, key=lambda v: int(v.version))[-1]

        print("All versions:", [v.version for v in versions])
        print(f"Latest version: {latest_version.version}, stage: {latest_version.current_stage}")
        print(f"Model source: {latest_version.source}")
        print(f"Run ID: {latest_version.run_id}")

        return self.load_model(name, latest_version.version, model_type=model_type)




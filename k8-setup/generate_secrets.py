import os
import base64
import yaml

# List the environment variables you want to include in the secret
secret_env_vars = [
    "MLFLOW_S3_ENDPOINT_URL",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "MLFLOW_SQLALCHEMY_POOL_SIZE",
    "MLFLOW_SQLALCHEMY_MAX_OVERFLOW",
    "S3_URL",
    "DATABASE_URL",
    "TRL_DATABASE_URL",
    "AWS_DEFAULT_REGION",
    "VASTAI_API_KEY",
    "MLFLOW_URI",
   "MLFLOW_FLASK_SERVER_SECRET_KEY",  # secret key for flask
   "MLFLOW_ADMIN_USERNAME",
   "MLFLOW_ADMIN_PASSWORD",
   "MLFLOW_PUBLIC_USERNAME",
   "SYNC_USERNAME",
   "SYNC_PASSWORD",
   "MLFLOW_PUBLIC_PASSWORD",
   "GF_SECURITY_ADMIN_PASSWORD",
   "MLFLOW_DB",
   "STATUS_DB",
   "AIRFLOW_DB"
]

secret_name = "platform-secrets"
namespace = "platform"

# Build the secret data dictionary with base64-encoded values
secret_data = {}
for var in secret_env_vars:
    value = os.environ.get(var)
    if value is None:
        raise ValueError(f"Environment variable {var} not found")
    # Kubernetes secrets are base64-encoded
    secret_data[var] = base64.b64encode(value.encode("utf-8")).decode("utf-8")

# Build the YAML structure
secret_yaml = {
    "apiVersion": "v1",
    "kind": "Secret",
    "metadata": {
        "name": secret_name,
        "namespace": namespace
    },
    "type": "Opaque",
    "data": secret_data
}

# Save to file
output_file = "platform-secrets.yml"
with open(output_file, "w") as f:
    yaml.dump(secret_yaml, f, sort_keys=False)

print(f"Kubernetes secret YAML generated: {output_file}")

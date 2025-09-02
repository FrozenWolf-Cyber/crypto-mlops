import os
import base64
import yaml

# List of environment variables to include directly in the secret
secret_env_vars = [
    "MLFLOW_S3_ENDPOINT_URL",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "MLFLOW_SQLALCHEMY_POOL_SIZE",
    "MLFLOW_SQLALCHEMY_MAX_OVERFLOW",
    "S3_URL",
    "DATABASE_URL",
    "AWS_DEFAULT_REGION",
    "VASTAI_API_KEY",
    "MLFLOW_URI",
    "MLFLOW_FLASK_SERVER_SECRET_KEY",  # secret key for flask
]

# Auth users: get from environment
auth_users = [
    {
        "username": os.environ.get("MLFLOW_ADMIN_USERNAME", "admin"),
        "password": os.environ.get("MLFLOW_ADMIN_PASSWORD", "admin123"),
        "role": "admin",
    },
    {
        "username": os.environ.get("MLFLOW_PUBLIC_USERNAME", "public"),
        "password": os.environ.get("MLFLOW_PUBLIC_PASSWORD", "readonly123"),
        "role": "readonly",
    },
]

secret_name = "platform-secrets"
namespace = "platform"

# Build secret data dictionary
secret_data = {}

# Standard vars
for var in secret_env_vars:
    value = os.environ.get(var)
    if value is None:
        raise ValueError(f"Environment variable {var} not found")
    secret_data[var] = base64.b64encode(value.encode("utf-8")).decode("utf-8")

# Users.yaml (basic-auth config)
users_yaml = {"users": auth_users}
users_yaml_str = yaml.dump(users_yaml, sort_keys=False)
secret_data["MLFLOW_AUTH_USERS"] = base64.b64encode(users_yaml_str.encode("utf-8")).decode("utf-8")

# Final Kubernetes secret YAML
secret_yaml = {
    "apiVersion": "v1",
    "kind": "Secret",
    "metadata": {
        "name": secret_name,
        "namespace": namespace,
    },
    "type": "Opaque",
    "data": secret_data,
}

# Save to file
output_file = "platform-secrets.yml"
with open(output_file, "w") as f:
    yaml.dump(secret_yaml, f, sort_keys=False)

print(f"Kubernetes secret YAML generated: {output_file}")

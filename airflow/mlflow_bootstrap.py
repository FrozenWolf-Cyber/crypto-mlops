import os
import time
from mlflow.server.auth.client import AuthServiceClient

tracking_uri = "http://127.0.0.1:5000"

admin_user = os.environ["MLFLOW_ADMIN_USERNAME"]
admin_pass = os.environ["MLFLOW_ADMIN_PASSWORD"]
public_user = os.environ["MLFLOW_PUBLIC_USERNAME"]
public_pass = os.environ["MLFLOW_PUBLIC_PASSWORD"]

client = AuthServiceClient(tracking_uri, username="admin", password="password1234")

# create admin
try:
    client.create_user(username=admin_user, password=admin_pass)
    client.assign_role(admin_user, "ADMIN")
except Exception as e:
    print("Admin creation skipped:", e)

# create readonly user
try:
    client.create_user(username=public_user, password=public_pass)
    client.assign_role(public_user, "READ_ONLY")
except Exception as e:
    print("Readonly creation skipped:", e)

print("✅ Users initialized")
client.delete_user("admin")
print("✅ Default admin user deleted")

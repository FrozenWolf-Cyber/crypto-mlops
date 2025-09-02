import os
from mlflow.server import get_app_client

tracking_uri = "http://127.0.0.1:5000"

# Bootstrap admin credentials (the default mlflow user)
os.environ["MLFLOW_TRACKING_USERNAME"] = "admin"
os.environ["MLFLOW_TRACKING_PASSWORD"] = "password1234"

admin_user = os.environ["MLFLOW_ADMIN_USERNAME"]
admin_pass = os.environ["MLFLOW_ADMIN_PASSWORD"]
public_user = os.environ["MLFLOW_PUBLIC_USERNAME"]
public_pass = os.environ["MLFLOW_PUBLIC_PASSWORD"]

# Get authenticated client
auth_client = get_app_client("basic-auth", tracking_uri=tracking_uri)

# Create admin user
try:
    user = auth_client.create_user(username=admin_user, password=admin_pass)
    auth_client.assign_role(admin_user, "ADMIN")
    print(f"✅ Created admin: {user.username} (ID: {user.id})")
except Exception as e:
    print("Admin creation skipped:", e)

# Create readonly user
try:
    user = auth_client.create_user(username=public_user, password=public_pass)
    auth_client.assign_role(public_user, "READ_ONLY")
    print(f"✅ Created readonly user: {user.username} (ID: {user.id})")
except Exception as e:
    print("Readonly creation skipped:", e)

# Delete bootstrap admin
try:
    auth_client.delete_user("admin")
    print("✅ Default bootstrap admin deleted")
except Exception as e:
    print("Admin deletion skipped:", e)

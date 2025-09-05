import os
from mlflow.server import get_app_client

tracking_uri = os.getenv("MLFLOW_URI")

# Bootstrap admin credentials (the default mlflow user)
os.environ["MLFLOW_TRACKING_USERNAME"] = "admin"
os.environ["MLFLOW_TRACKING_PASSWORD"] = "password1234"

admin_user = os.environ["MLFLOW_ADMIN_USERNAME"]
admin_pass = os.environ["MLFLOW_ADMIN_PASSWORD"]
public_user = os.environ["MLFLOW_PUBLIC_USERNAME"]
public_pass = os.environ["MLFLOW_PUBLIC_PASSWORD"]

print("Admin user:", admin_user)
print("Public user:", public_user)
print("Admin pass:", admin_pass)
print("Public pass:", public_pass)
# Get authenticated client
auth_client = get_app_client("basic-auth", tracking_uri=tracking_uri)

# Create admin user
try:
    auth_client.update_user_password(username="admin", password=admin_pass)
    print(f"✅ Updated admin")
    
except Exception as e:
    print("Admin updation skipped:", e)

os.environ["MLFLOW_TRACKING_PASSWORD"] = admin_pass

# Get authenticated client
auth_client = get_app_client("basic-auth", tracking_uri=tracking_uri)

# Create readonly user
try:
    user = auth_client.create_user(username=public_user, password=public_pass)
    # auth_client.update_user_admin(username=public_user, is_admin=False)
    
    print(f"✅ Created readonly user: {user.username} (ID: {user.id})")
except Exception as e:
    print("Readonly creation skipped:", e)


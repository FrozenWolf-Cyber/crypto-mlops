
# Installation Guide — Crypto MLOps Dashboard

**Live Demo:** [https://crypto.gokuladethya.uk/](https://crypto.gokuladethya.uk/)

## 1. Prerequisites

Before starting, ensure the following are installed:

* **k3s / Kubernetes cluster**
* **Docker**
* **Helm**
* **Python 3+** (used for generating Kubernetes secrets)

---

## 2. Setup Kubernetes Namespace

Create and switch to the dedicated namespace:

```bash
kubectl create namespace platform 
kubectl config set-context --current --namespace=platform
```

---

## 3. Build and Push Docker Images

Navigate to the appropriate directories and build images.

### FastAPI Model Serving Image

```bash
cd utils
docker build -t fastapi-ml:latest -f dockerfiles/modelserve.dockerfile .
docker tag fastapi-ml:latest frozenwolf2003/fastapi-ml:latest
docker push frozenwolf2003/fastapi-ml:latest
```

### Dashboard Backend Image

```bash
docker build -t crypto-backend:latest -f dockerfiles/backend.dockerfile .
docker tag crypto-backend:latest frozenwolf2003/crypto-backend:latest
docker push frozenwolf2003/crypto-backend:latest
```

### Airflow Worker Image

```bash
cd utils.dockerfile
docker build -f worker.dockerfile -t mlops-airflow .
docker tag mlops-airflow:latest frozenwolf2003/mlops-airflow:latest
docker push frozenwolf2003/mlops-airflow:latest
```

### Producer–Consumer Image

```bash
docker build -f producer_consumer.dockerfile -t producer_consumer .
docker tag producer_consumer:latest frozenwolf2003/producer_consumer:latest
docker push frozenwolf2003/producer_consumer:latest
```

---

## 4. Configure Secrets

Set the following environment variables before generating the Kubernetes secrets file:

```bash
# R2 Storage Credentials (used by MLflow and S3)
export MLFLOW_S3_ENDPOINT_URL=https://****.r2.cloudflarestorage.com
export S3_URL=https://****.r2.cloudflarestorage.com 
export AWS_ACCESS_KEY_ID=****
export AWS_SECRET_ACCESS_KEY=****

export MLFLOW_SQLALCHEMY_POOL_SIZE=2
export MLFLOW_SQLALCHEMY_MAX_OVERFLOW=0

# Secret MLflow API Endpoint (non-Captcha endpoint used for training logs)
export MLFLOW_URI=https://****.gokuladethya.uk

# Databases
export DATABASE_URL="postgresql+psycopg2://****"
export AWS_DEFAULT_REGION=auto
export AIRFLOW_DB="postgresql+psycopg2://****"
export MLFLOW_DB="postgresql://****"
export STATUS_DB="postgresql+psycopg2://****"

# Keys and Credentials
export VASTAI_API_KEY=
export MLFLOW_ADMIN_USERNAME=
export MLFLOW_ADMIN_PASSWORD=
export MLFLOW_PUBLIC_USERNAME='012345678910'
export MLFLOW_PUBLIC_PASSWORD='012345678910'
export MLFLOW_FLASK_SERVER_SECRET_KEY=
export GF_SECURITY_ADMIN_PASSWORD=
```

### Generate and Apply Secrets

```bash
mkdir /mnt/shared-airflow-custom/
mkdir /mnt/shared/
mkdir /mnt/shared/jobs

sudo chmod -R 777 /mnt/
sudo chmod -R 777 /mnt/shared/
sudo chmod -R 777 /mnt/shared-airflow-custom/
sudo chmod -R 777 /mnt/shared/*
sudo chmod -R 777 /mnt/shared/jobs

python3 generate_secrets.py
kubectl apply -f platform-secrets.yml
```

---

## 5. Deploy Core Components

```bash
kubectl apply -f pvc.yaml                # Persistent Volumes
kubectl apply -f ml-flow.yaml            # Deploy MLflow (wait for it to start fully)
docker compose -f kafka.yml up -d        # Start Kafka locally
kubectl apply -f kafka-service.yaml      # Expose Kafka to K8s
kubectl apply -f producer-consumer.yaml  # Launch producer-consumer pod
```

---

## 6. Ingress and Monitoring Setup

### Directories and Permissions

```bash
sudo mkdir -p /mnt/shared/prometheus
sudo mkdir -p /mnt/shared/grafana

sudo chown -R 65534:65534 /mnt/shared/prometheus  # Prometheus UID
sudo chown -R 472:472 /mnt/shared/grafana         # Grafana UID
```

### Install NGINX Ingress Controller

```bash
helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx
helm repo update

kubectl create namespace ingress-nginx
helm upgrade --install ingress-nginx ingress-nginx/ingress-nginx \
  --namespace ingress-nginx \
  --set controller.metrics.enabled=true \
  --set controller.metrics.serviceMonitor.enabled=true \
  --set controller.metrics.serviceMonitor.additionalLabels.release="prometheus"
```

### Install Cert Manager

```bash
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.14.4/cert-manager.yaml
```

---

## 7. Monitoring (Prometheus + Grafana)

Install using **kube-prometheus-stack**:

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

helm install prometheus prometheus-community/kube-prometheus-stack -n prometheus --create-namespace
helm upgrade prometheus prometheus-community/kube-prometheus-stack \
  --namespace prometheus  \
  --set prometheus.prometheusSpec.podMonitorSelectorNilUsesHelmValues=false \
  --set prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues=false
```

### Grafana Setup

Default password: `prom-operator`

* Open Grafana → **Configuration → Data Sources → Add Data Source**
* Select **Prometheus**
* Use Prometheus Cluster IP (e.g., `http://10.102.72.134:9090`)
* Import dashboard:

  * Go to **+ → Import**
  * Paste JSON from [Nginx Dashboard JSON](https://raw.githubusercontent.com/kubernetes/ingress-nginx/main/deploy/grafana/dashboards/nginx.json)
  * Select the Prometheus data source

---

## 8. Configure MLflow Ingress

Edit `mlflow-ingress.yaml` and replace `<FILLER>` with your **MLFLOW_URI** value.
Then apply:

```bash
kubectl apply -f mlflow-ingress.yaml
```

> Before enabling Cloudflare proxying, ensure the endpoint works directly.
> After confirming, turn on proxy mode in Cloudflare.

---

## 9. Bootstrap MLflow Users

```bash
python3 mlflow_bootstrap.py
```

Creates:

* Admin user (from `MLFLOW_ADMIN_USERNAME` / `MLFLOW_ADMIN_PASSWORD`)
* Public view user (`MLFLOW_PUBLIC_USERNAME` / `MLFLOW_PUBLIC_PASSWORD`)

---

## 10. Grafana Configuration

```bash
kubectl apply -f grafana-config.yaml
kubectl apply -f grafanaingress.yaml
```

### Enable Anonymous Access

```bash
kubectl edit configmap prometheus-grafana -n prometheus
```

Modify `grafana.ini`:

```ini
[auth.anonymous]
enabled = true
org_name = Main Org.
org_role = Viewer
hide_version = true
device_limit = 100

[feature_toggles]
enable = displayAnonymousStats
```

Then apply and restart:

```bash
kubectl apply -f grafana-config.yaml
kubectl rollout restart deployment prometheus-grafana -n prometheus
```

---

## 11. Deploy Remaining Components

### FastAPI Model Hosting

```bash
kubectl apply -f fast-api.yaml
```

### Airflow Worker and Scheduler

```bash
kubectl apply -f pod_template.yaml

helm repo add apache-airflow https://airflow.apache.org
helm install airflow apache-airflow/airflow -n platform \
  -f values.yaml \
  --set images.airflow.repository=frozenwolf2003/mlops-airflow \
  --set images.airflow.tag=latest \
  --set images.airflow.pullPolicy=Always
```

Check the UI:

```bash
kubectl port-forward svc/airflow-api-server 8080:8080
```

Visit: [http://localhost:8080](http://localhost:8080)

---

## 12. Dashboard Backend and Ingress

```bash
kubectl apply -f backend.yaml
```

> This launches the main **Crypto MLOps Dashboard backend** and its ingress service.

---

Would you like me to include a **“System Architecture Diagram”** section at the end (e.g., showing Kafka ↔ FastAPI ↔ MLflow ↔ Airflow ↔ Grafana)? It would make the README much more informative for new contributors.

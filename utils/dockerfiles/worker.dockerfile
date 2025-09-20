FROM apache/airflow:3.0.6

WORKDIR /app

# Install system dependencies (jq, curl, etc.)
# USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    jq curl \
 && rm -rf /var/lib/apt/lists/*

# Back to airflow user
# USER airflow

# Copy Python requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir  -r requirements.txt

# Install Playwright and its Python package
RUN pip install --no-cache-dir  playwright
RUN playwright install
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}"
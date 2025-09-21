FROM apache/airflow:3.0.2

WORKDIR /app

# Install system dependencies (jq, curl, etc.)
# USER root
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     jq curl \
#  && rm -rf /var/lib/apt/lists/*

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
USER root
RUN sudo playwright install-deps
RUN sudo apt-get update && sudo apt-get install -y \
    libglib2.0-0 \
    libnspr4 \
    libnss3 \
    libdbus-1-3 \
    libatk1.0-0 \
    libatspi2.0-0 \
    libx11-6 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libxcb1 \
    libxkbcommon0 \
    libasound2

USER airflow
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}"
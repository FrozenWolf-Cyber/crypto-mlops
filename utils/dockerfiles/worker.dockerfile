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
# Install Playwright and its browsers
RUN pip install --no-cache-dir playwright
USER root
RUN playwright install
RUN sudo playwright install-deps

# Install system dependencies required by Playwright
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget curl gnupg ca-certificates \
    libglib2.0-0 libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 libatspi2.0-0 \
    libcups2 libx11-6 libxcomposite1 libxdamage1 libxext6 libxfixes3 libxrandr2 \
    libgbm1 libpango-1.0-0 libxkbcommon0 libasound2 fonts-liberation libgtk-3-0 \
    libxshmfence1 libwayland-client0 libwayland-egl1 libdrm2 libexpat1 \
    libxrender1 libxinerama1 libharfbuzz0b libfribidi0 \
    && rm -rf /var/lib/apt/lists/*


USER airflow
# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt


RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}"
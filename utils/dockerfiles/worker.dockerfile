FROM apache/airflow:3.0.2

WORKDIR /app

# 1️⃣ Install system dependencies for Playwright as root
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget curl gnupg ca-certificates \
    libglib2.0-0 libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 libatspi2.0-0 \
    libcups2 libx11-6 libxcomposite1 libxdamage1 libxext6 libxfixes3 libxrandr2 \
    libgbm1 libpango-1.0-0 libxkbcommon0 libasound2 fonts-liberation libgtk-3-0 \
    libxshmfence1 libwayland-client0 libwayland-egl1 libdrm2 libexpat1 \
    libxrender1 libxinerama1 libharfbuzz0b libfribidi0 \
    && rm -rf /var/lib/apt/lists/*

RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*
RUN apt-get install -y locales
RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

# 2️⃣ Install Python dependencies as airflow user
USER airflow
COPY requirements.txt .

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir playwright

# 3️⃣ Install Playwright browsers as root to a shared folder
USER root
ENV PLAYWRIGHT_BROWSERS_PATH=/opt/playwright
RUN mkdir -p /opt/playwright
RUN chown -R airflow: /opt/playwright
RUN playwright install --with-deps

# 4️⃣ Switch to airflow user and point to the shared browsers
USER airflow
ENV PLAYWRIGHT_BROWSERS_PATH=/opt/playwright

# 5️⃣ Test Playwright works
RUN python - <<EOF
from playwright.sync_api import sync_playwright
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    print("✅ Chromium launched successfully")
    browser.close()
EOF

COPY past_news_scrape.py .
COPY scrape.py .
RUN mkdir my_repo_clone
RUN git clone https://github.com/FrozenWolf-Cyber/crypto-mlops my_repo_clone
RUN cp -r my_repo_clone/* ${AIRFLOW_HOME}/
RUN echo $AIRFLOW_HOME
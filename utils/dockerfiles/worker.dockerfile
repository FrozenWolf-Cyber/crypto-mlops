FROM apache/airflow:3.0.2

WORKDIR /app

# Step 1: root installs system dependencies
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget curl gnupg ca-certificates \
    libglib2.0-0 libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 libatspi2.0-0 \
    libcups2 libx11-6 libxcomposite1 libxdamage1 libxext6 libxfixes3 libxrandr2 \
    libgbm1 libpango-1.0-0 libxkbcommon0 libasound2 fonts-liberation libgtk-3-0 \
    libxshmfence1 libwayland-client0 libwayland-egl1 libdrm2 libexpat1 \
    libxrender1 libxinerama1 libharfbuzz0b libfribidi0 \
    && rm -rf /var/lib/apt/lists/*

# Step 2: airflow user installs Python packages
USER airflow
COPY requirements.txt .
RUN pip install --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt \
 && pip install --no-cache-dir playwright

# Step 3: root installs browsers & dependencies for Playwright
USER root
RUN sudo playwright install-deps && sudo playwright install

# Step 4: verify Playwright works at build time
RUN python - <<EOF
from playwright.sync_api import sync_playwright
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    print("✅ Chromium launched successfully")
    browser.close()
EOF

# Step 5: switch back to airflow
USER airflow
ENV PLAYWRIGHT_BROWSERS_PATH=/usr/lib/playwright
RUN python - <<EOF
from playwright.sync_api import sync_playwright
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    print("✅ Chromium launched successfully")
    browser.close()
EOF

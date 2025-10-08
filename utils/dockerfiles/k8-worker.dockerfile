# Use a slim Python base instead of Airflow
FROM python:3.11-slim

WORKDIR /app

# 1️⃣ Install system dependencies for Playwright as root
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget curl gnupg ca-certificates \
    libglib2.0-0 libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 libatspi2.0-0 \
    libcups2 libx11-6 libxcomposite1 libxdamage1 libxext6 libxfixes3 libxrandr2 \
    libgbm1 libpango-1.0-0 libxkbcommon0 libasound2 fonts-liberation libgtk-3-0 \
    libxshmfence1 libwayland-client0 libwayland-egl1 libdrm2 libexpat1 \
    libxrender1 libxinerama1 libharfbuzz0b libfribidi0 \
    locales \
    && rm -rf /var/lib/apt/lists/*

# 2️⃣ Setup locale
RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

# 3️⃣ Install Python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt \
 && pip install --no-cache-dir playwright

# 4️⃣ Install Playwright browsers
ENV PLAYWRIGHT_BROWSERS_PATH=/opt/playwright
RUN mkdir -p /opt/playwright \
 && playwright install --with-deps

# 5️⃣ Test Playwright works
RUN python - <<EOF
from playwright.sync_api import sync_playwright
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    print("✅ Chromium launched successfully")
    browser.close()
EOF

# 6️⃣ Copy project files
COPY past_news_scrape.py .
COPY scrape.py .

# (Optional) Run script on build – consider ENTRYPOINT instead
RUN python past_news_scrape.py
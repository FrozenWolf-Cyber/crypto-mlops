FROM apache/airflow:3.0.2-python3.12

WORKDIR /app

# Install minimal system dependencies
# RUN sudo apt-get update && sudo apt-get install -y git build-essential && rm -rf /var/lib/apt/lists/*

# Copy Python requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Install Playwright and its Python package
RUN pip install playwright
RUN playwright install

CMD ["bash"]

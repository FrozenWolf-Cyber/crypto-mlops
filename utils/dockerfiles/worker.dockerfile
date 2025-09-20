FROM apache/airflow

WORKDIR /app


# Copy Python requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Install Playwright and its Python package
RUN pip install playwright
RUN playwright install

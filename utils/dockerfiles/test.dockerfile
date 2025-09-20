FROM apache/airflow:3.0.6
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" lxml
FROM apache/airflow:3.1.1
RUN pip install --no-cache-dir huggingface_hub requests minio
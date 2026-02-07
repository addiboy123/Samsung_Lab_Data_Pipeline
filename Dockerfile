FROM apache/airflow:2.8.1-python3.10

USER root

# Install AWS CLI
RUN apt-get update && \
    apt-get install -y awscli && \
    apt-get clean

USER airflow

# Install Python dependencies
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

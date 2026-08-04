FROM apache/airflow:2.9.3

USER root

# operational utilities for diagnostics and health checks
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    openjdk-17-jre-headless \
  && apt-get clean && rm -rf /var/lib/apt/lists/*

# Python dependecies for Airflow providers
COPY requirements.txt /requirements.txt
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt

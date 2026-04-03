FROM apache/airflow:2.10.4-python3.11

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

COPY --chown=airflow:root requirements-phase1.txt /tmp/requirements-phase1.txt

RUN pip install --no-cache-dir pyspark==4.1.1 \
    && pip install --no-cache-dir -r /tmp/requirements-phase1.txt

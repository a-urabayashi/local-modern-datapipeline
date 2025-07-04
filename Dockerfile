FROM apache/airflow:2.7.1-python3.11

USER root

RUN apt-get update && apt-get install -y openjdk-11-jdk wget gcc python3-dev curl git procps bash gnupg lsb-release docker.io && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PATH=$JAVA_HOME/bin:$PATH
# Install nbconvert and its dependencies
USER airflow
COPY ./requirements.txt /opt/airflow/requirements.txt
COPY ./scripts/ /app/scripts/
RUN pip install -r /opt/airflow/requirements.txt
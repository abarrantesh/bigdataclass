FROM python:3.10-slim

WORKDIR /app
COPY . /app

RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    pip install --no-cache-dir pyspark pytest && \
    apt-get clean;

ENV JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
CMD ["bash"]

FROM python:3.10-slim

WORKDIR /app
COPY . /app

RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    pip install --no-cache-dir pyspark pytest && \
    apk add bash && \
    apk add nano && \
    apk add postgresql-client && \
    apk add python3 && \
    pip3 install --upgrade pip && \
    pip3 install pyspark && \
    pip3 install pytest && \
    apt-get clean;

ENV JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
CMD ["bash"]

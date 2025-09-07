FROM bitnami/spark:4.0.0

USER root


# basic tools
RUN mkdir -p /var/lib/apt/lists/partial && \
    apt-get update && \
    apt-get install -y \
    curl \
    gnupg \
    ca-certificates \
    ncurses-bin \
    wget

# add SBT repo and key then install
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" > /etc/apt/sources.list.d/sbt.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x99E82A75642AC823" | apt-key add - && \
    apt-get update && \
    apt-get install -y sbt


# adding kafka jars
RUN cd /opt/bitnami/spark/jars && \
    wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.0/spark-sql-kafka-0-10_2.13-4.0.0.jar && \
    wget https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.0.0/spark-token-provider-kafka-0-10_2.13-4.0.0.jar && \
    wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.0/kafka-clients-3.9.0.jar && \
    wget https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar


# aws jars for minio
RUN wget https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.25.10/bundle-2.25.10.jar -P /opt/bitnami/spark/jars/
# RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.0/hadoop-aws-3.4.0.jar -P /opt/bitnami/spark/jars/

# set working directory
WORKDIR /app

# install UV
RUN pip install --no-cache-dir uv

# copy project files
COPY pyproject.toml uv.lock /app/
# COPY ./jars/ /opt/bitnami/spark/jars/

# sync dependencies
RUN uv sync
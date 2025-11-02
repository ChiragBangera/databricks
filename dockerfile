FROM apache/spark:4.0.0-scala2.13-java17-python3-ubuntu

USER root

# Basic tools
RUN apt-get update && \
    apt-get install -y \
    curl \
    gnupg \
    ca-certificates \
    ncurses-bin \
    wget && \
    rm -rf /var/lib/apt/lists/*

# Add SBT repo and key then install
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" > /etc/apt/sources.list.d/sbt.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x99E82A75642AC823" | apt-key add - && \
    apt-get update && \
    apt-get install -y sbt && \
    rm -rf /var/lib/apt/lists/*

# Adding Kafka jars
RUN cd /opt/spark/jars && \
    wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.0/spark-sql-kafka-0-10_2.13-4.0.0.jar && \
    wget https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.0.0/spark-token-provider-kafka-0-10_2.13-4.0.0.jar && \
    wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.0/kafka-clients-3.9.0.jar && \
    wget https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar

# Delta Lake jars
ENV DELTA_VERSION=4.0.0
ENV SCALA_VERSION=2.13
RUN wget https://repo1.maven.org/maven2/io/delta/delta-spark_${SCALA_VERSION}/${DELTA_VERSION}/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar \
    -P /opt/spark/jars && \
    wget https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar \
    -P /opt/spark/jars

# AWS jars for MinIO
RUN wget https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.25.10/bundle-2.25.10.jar -P /opt/spark/jars/ && \
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.0/hadoop-aws-3.4.0.jar -P /opt/spark/jars/


# Set working directory
WORKDIR /app

# Install UV
RUN pip install --no-cache-dir uv

# Copy project files
COPY pyproject.toml uv.lock /app/

# Sync dependencies
RUN uv sync

# Switch back to spark user (optional, for security)
# USER spark

FROM apache/airflow:2.7.3-python3.10

USER root

# Install OpenJDK 17 and procps (provides ps command)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME correctly
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow

# Install Python packages
RUN pip install --no-cache-dir pyspark==3.5.0 psycopg2-binary

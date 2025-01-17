# Start from the official Flink image
FROM flink:1.19.1-scala_2.12-java11
###############################################
## Add Startup Script And Make Executable
###############################################

# COPY ./initiate-flink.sh /opt/flink/bin/initiate-vars.sh
# RUN chmod +x /opt/flink/bin/initiate-vars.sh

###############################################
## Download Neccessary Jars to Flink Class Path
###############################################

# Set environment variables to avoid interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# Install necessary dependencies
RUN apt-get -qq update && \
    apt-get -qq install -y --no-install-recommends sudo curl openjdk-11-jdk && \
    apt-get -qq clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV PATH=$JAVA_HOME/bin:$PATH

# Install Python from the python:3.10-bullseye image
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        python3.10 \
        python3-pip \
        python3-dev \
        python3-setuptools \
        python3-venv \
        build-essential \
        libffi-dev \
        libz-dev \
        gcc-aarch64-linux-gnu \
        unzip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set Python as default
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.10 1

# Set environment variables for Python development
ENV PYTHON_INCLUDE_DIR=/usr/include/python3.10
ENV C_INCLUDE_PATH=$PYTHON_INCLUDE_DIR:$C_INCLUDE_PATH
ENV CPLUS_INCLUDE_PATH=$PYTHON_INCLUDE_DIR:$CPLUS_INCLUDE_PATH

# Verify installations
RUN python3 --version && pip3 --version && aarch64-linux-gnu-gcc --version

# Optional: Install PyFlink or other Python libraries
RUN pip install --no-cache-dir pemja
RUN pip install apache-flink==1.19.1

## Iceberg Flink Library
RUN curl -L https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.19/1.6.0/iceberg-flink-runtime-1.19-1.6.0.jar -o /opt/flink/lib/iceberg-flink-runtime-1.19-1.6.0.jar

## Hive Flink Library
RUN curl -L https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-hive-2.3.9_2.12/1.19.2/flink-sql-connector-hive-2.3.9_2.12-1.19.2.jar -o /opt/flink/lib/flink-sql-connector-hive-2.3.9_2.12-1.19.2.jar

## Hadoop Common Classes
RUN curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/2.8.3/hadoop-common-2.8.3.jar -o /opt/flink/lib/hadoop-common-2.8.3.jar

## Hadoop AWS Classes
RUN curl -L https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar -o /opt/flink/lib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar

## AWS Bundled Classes
RUN curl -L https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.20.18/bundle-2.20.18.jar -o /opt/flink/lib/bundle-2.20.18.jar

## Install Nano to edit files
RUN apt update && apt install -y nano

COPY download_gdelt_event.sh .
COPY flink-entrypoint.sh .
COPY gdelt_to_flink_iceberg.py .
COPY provisioned-table.py .

HEALTHCHECK --retries=120 --interval=1s \
  CMD ls /tmp/ready || exit 1

# Set entrypoint to start Flink
RUN chmod +x flink-entrypoint.sh
RUN chmod +x download_gdelt_event.sh
CMD ["./flink-entrypoint.sh"]

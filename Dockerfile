# Dockerfile
# Base image: Official Airflow 2.6.0 with Python 3.9
FROM apache/airflow:2.6.0-python3.9

# Copy Python dependencies file
COPY requirements.txt /tmp/requirements.txt

# Switch to root user to install system packages
USER root

# Install Java 17 (needed for Spark/PySpark)
# This is required for AWS Glue local testing
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Switch back to airflow user (security best practice)
USER airflow

# Install Python dependencies
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Explanation of this Dockerfile:
# ================================
# 1. We start with official Airflow image (already has Airflow installed)
# 2. We add Java because:
#    - AWS Glue uses Apache Spark
#    - Spark runs on JVM (Java Virtual Machine)
#    - Even though we write Python, Spark needs Java underneath
# 3. We install our Python libraries (boto3 for AWS, pandas for data, etc.)
# 4. Everything runs as 'airflow' user (not root) for security
FROM apache/airflow:2.8.1-python3.11

USER root

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        wget \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements and install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy custom scripts and plugins
COPY scripts/ /opt/airflow/scripts/
COPY plugins/ /opt/airflow/plugins/

# Set permissions
RUN chmod -R 755 /opt/airflow/scripts
RUN chmod -R 755 /opt/airflow/plugins

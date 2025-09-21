FROM apache/airflow:2.8.1-python3.11

USER root

# Install system dependencies including uv
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        wget \
    && curl -LsSf https://astral.sh/uv/install.sh | sh \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Add uv to PATH
ENV PATH="/root/.cargo/bin:$PATH"

# Copy pyproject.toml and install Python dependencies with uv
COPY pyproject.toml /pyproject.toml
RUN uv pip install --system -e .

# Copy custom scripts and plugins
COPY scripts/ /opt/airflow/scripts/
COPY plugins/ /opt/airflow/plugins/

# Set permissions
RUN chmod -R 755 /opt/airflow/scripts
RUN chmod -R 755 /opt/airflow/plugins


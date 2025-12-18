# Use the official Airflow 3 image (latest stable as of Dec 2025)
FROM apache/airflow:3.1.5

# Set working directory
WORKDIR /opt/airflow

# Switch to root to install system-level dependencies
USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user for security and Python package installation
USER airflow

# Copy local requirements
COPY requirements.txt .

# Copy main folders into dir
COPY dags /opt/airflow/dags
COPY etl_scripts /opt/airflow/etl_scripts

# Airflow 3 encourages using 'uv' for significantly faster installations
# If you prefer standard pip, use: RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

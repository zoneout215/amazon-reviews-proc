# Use Apache Airflow as the base image
FROM --platform=linux/amd64 apache/airflow:2.7.3-python3.9

# Set environment variables
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

# Set the working directory for convenience
WORKDIR /opt/airflow

# Switch to ROOT user for installing mandatory packages
USER root

# # Install mandatory packages
RUN apt-get update \
       && apt-get install -y --no-install-recommends \
              vim \
       && apt-get install -y --no-install-recommends curl \
       && apt-get autoremove -yqq --purge \
       && apt-get clean \
       && apt-get install -y libpq-dev gcc \
       && rm -rf /var/lib/apt/lists/* \ 
       && apt-get install -y --no-install-recommends


# Copy entrypoint script to the container
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Switch back to the default Airflow user
USER airflow

# Copy requirements.txt into the Docker container
COPY requirements.txt /opt/airflow/requirements.txt
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
# Install needed Python packages
RUN uv venv \
 && source /opt/airflow/.venv/bin/activate \
 && uv pip install --upgrade pip \
 && uv pip install --trusted-host pypi.python.org -r /opt/airflow/requirements.txt \
 && mkdir -p /tmp/downloads/data

# Copy your dags folder to the container
COPY airflow/dags /opt/airflow/dags

# Run the ini script
ENTRYPOINT ["/entrypoint.sh"]

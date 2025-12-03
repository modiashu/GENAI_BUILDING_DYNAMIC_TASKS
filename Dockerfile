FROM apache/airflow:3.0.0

# Set proxy environment variables for build
ARG NO_PROXY=localhost,127.0.0.1

ENV no_proxy=${NO_PROXY} \
    NO_PROXY=${NO_PROXY}

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
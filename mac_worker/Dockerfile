# Use Python base image compatible with ARM64
FROM --platform=linux/arm64 python:3.12-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive \
    RAY_verbose_spill_logs=1 \
    RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE=1 \
    RAY_BACKEND_LOG_LEVEL=debug \
    RAY_enable_mac_large_object_store=1 \
    RAY_RAYLET_STARTUP_TIMEOUT_MS=60000

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    net-tools \
    iputils-ping \
    netcat-traditional \
    procps \
    htop \
    strace \
    redis-tools \
    && rm -rf /var/lib/apt/lists/*

# Install Ray and other Python dependencies
RUN pip install --no-cache-dir \
    "ray[default]==2.37.0" \
    numpy \
    pandas \
    psutil

# Create directories for Ray and enable logging
RUN mkdir -p /tmp/ray \
    && mkdir -p /ray-storage \
    && mkdir -p /tmp/ray/session_latest/logs \
    && mkdir -p /tmp/ray/debug \
    && chmod 777 -R /tmp/ray

# Create Python script for cluster check
COPY check_cluster.py /check_cluster.py
RUN chmod +x /check_cluster.py

# Create a script to start the Ray worker
COPY start-worker.sh /start-worker.sh
RUN chmod +x /start-worker.sh

ENTRYPOINT ["/start-worker.sh"]
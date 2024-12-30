# Use NVIDIA CUDA base image with Python 3.12
FROM nvidia/cuda:12.0.0-base-ubuntu20.04

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive
ENV RAY_HEAD_NODE=67.205.167.215
ENV PATH="/usr/local/cuda/bin:${PATH}"
ENV LD_LIBRARY_PATH="/usr/local/cuda/lib64:${LD_LIBRARY_PATH}"

# Install Python 3.12 and other dependencies
RUN apt-get update && apt-get install -y \
    software-properties-common \
    && add-apt-repository ppa:deadsnakes/ppa \
    && apt-get update && apt-get install -y \
    python3.12 \
    python3.12-distutils \
    python3.12-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install pip for Python 3.12
RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py \
    && python3.12 get-pip.py \
    && rm get-pip.py

# Create Ray temporary directory
RUN mkdir -p /tmp/ray_tmp && chmod 777 /tmp/ray_tmp

# Install Python packages
RUN python3.12 -m pip install --no-cache-dir \
    "ray[default]==2.37.0" \
    psutil \
    nvidia-ml-py3 \
    numpy \
    torch

# Copy the provider node script
COPY provider_node.py /app/provider_node.py

# Make script executable
RUN chmod +x /app/provider_node.py

# Set working directory
WORKDIR /app

# Set the entrypoint script
ENTRYPOINT ["python3.12", "provider_node.py"]
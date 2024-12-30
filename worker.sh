#!/bin/bash

# Check if Docker is installed
if ! command -v docker &> /dev/null
then
    echo "Docker is not installed. Please install Docker and try again."
    exit 1
fi

# Check if the required arguments are provided
if [ "$#" -lt 4 ]; then
    echo "Usage: $0 <head_node_ip> <provider_id> <username> <password>"
    exit 1
fi

HEAD_NODE_IP=$1
PROVIDER_ID=$2
USERNAME=$3
PASSWORD=$4

# Check for NVIDIA GPU directly
if command -v nvidia-smi &> /dev/null; then
    echo "NVIDIA GPU detected. Checking GPU details..."
    nvidia-smi
    GPU_FLAG="--gpus all"
    # Also check NVIDIA Docker runtime
    if ! docker info | grep -q "nvidia"; then
        echo "Warning: NVIDIA Docker runtime not found. Installing NVIDIA Container Toolkit..."
        distribution=$(. /etc/os-release;echo $ID$VERSION_ID)
        curl -s -L https://nvidia.github.io/nvidia-docker/gpgkey | sudo apt-key add -
        curl -s -L https://nvidia.github.io/nvidia-docker/$distribution/nvidia-docker.list | sudo tee /etc/apt/sources.list.d/nvidia-docker.list
        sudo apt-get update && sudo apt-get install -y nvidia-container-toolkit
        sudo systemctl restart docker
    fi
else
    echo "No NVIDIA GPU detected or nvidia-smi not found."
    GPU_FLAG=""
fi

# Get the auth token
TOKEN_RESPONSE=$(curl -s -X POST "http://67.205.167.215:8000/token" \
     -H "Content-Type: application/x-www-form-urlencoded" \
     -d "username=$USERNAME&password=$PASSWORD")

# Extract the access token using string manipulation
AUTH_TOKEN=$(echo $TOKEN_RESPONSE | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')

if [ -z "$AUTH_TOKEN" ]; then
    echo "Failed to get auth token. Please check your credentials."
    echo "Server response: $TOKEN_RESPONSE"
    exit 1
fi

echo "Successfully obtained auth token."

# Pull the custom Ray image
echo "Pulling Ray image..."
docker pull piyushaaryan/ioc:1.0

# Check if running in Windows/MinTTY
if [ -n "$MSYSTEM" ] || [ -n "$MINGW_PREFIX" ]; then
    echo "Windows environment detected. Using winpty..."
    DOCKER_PREFIX="winpty docker"
else
    DOCKER_PREFIX="docker"
fi

echo "Starting Ray worker container with GPU support: $GPU_FLAG"

# Run the Ray container
$DOCKER_PREFIX run --rm \
    --name ray-worker-${PROVIDER_ID} \
    --network host \
    --shm-size=1gb \
    $GPU_FLAG \
    -e RAY_ADDRESS="ray://${HEAD_NODE_IP}:10001" \
    -e PROVIDER_ID="${PROVIDER_ID}" \
    -e AUTH_TOKEN="${AUTH_TOKEN}" \
    -e NVIDIA_VISIBLE_DEVICES=all \
    -e NVIDIA_DRIVER_CAPABILITIES=all \
    piyushaaryan/ioc:1.1

# File ends here
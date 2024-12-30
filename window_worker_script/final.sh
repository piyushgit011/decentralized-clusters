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

# Run the Ray container
docker run --rm \
    --name ray-worker-${PROVIDER_ID} \
    --network host \
    --gpus all \
    --shm-size=1gb \
    -e RAY_ADDRESS="ray://${HEAD_NODE_IP}:10001" \
    -e PROVIDER_ID="${PROVIDER_ID}" \
    -e AUTH_TOKEN="${AUTH_TOKEN}" \
    piyushaaryan/ioc:1.1

# File ends here
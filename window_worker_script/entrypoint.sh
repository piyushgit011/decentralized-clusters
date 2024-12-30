#!/bin/bash

echo "Starting Ray worker node..."

# Test GPU availability
echo "Testing GPU support..."
python3 test_gpu.py

# Run authentication
echo "Authenticating..."
python3 authenticate.py
if [ $? -eq 0 ]; then
    # Report resources
    echo "Reporting resources..."
    python3 report_resources.py
    
    # Start Ray
    echo "Starting Ray..."
    ray start --address="$RAY_ADDRESS" --block
else
    echo "Authentication failed"
    exit 1
fi
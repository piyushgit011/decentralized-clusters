#!/bin/bash

# Default values
RAY_HEAD_IP=${RAY_HEAD_IP:-"localhost"}
RAY_HEAD_PORT=${RAY_HEAD_PORT:-"6379"}
NUM_CPUS=${NUM_CPUS:-"0"}
OBJECT_STORE_MEMORY=${OBJECT_STORE_MEMORY:-"1000000000"}
NODE_IP_ADDRESS=${NODE_IP_ADDRESS:-"192.168.1.10"}

# Enable detailed logging
export RAY_BACKEND_LOG_LEVEL=debug
export RAY_DISABLE_DOCKER_CPU_WARNING=1

# Print debug information
echo "Debugging information:"
echo "Head node IP: $RAY_HEAD_IP"
echo "Head node port: $RAY_HEAD_PORT"
echo "Number of CPUs: $NUM_CPUS"
echo "Object store memory: $OBJECT_STORE_MEMORY bytes"
echo "Using node IP address: $NODE_IP_ADDRESS"

# Check cluster before starting
echo "Checking Ray cluster connectivity..."
python /check_cluster.py
CLUSTER_CHECK=$?
if [ $CLUSTER_CHECK -ne 0 ]; then
    echo "Warning: Cluster check failed, but continuing..."
fi

# Network connectivity test
echo "Network connectivity test:"
ping -c 2 $RAY_HEAD_IP

# Test Redis connectivity and get cluster info
echo "Testing Redis connectivity..."
if command -v redis-cli &> /dev/null; then
    echo "Checking Redis info:"
    redis-cli -h $RAY_HEAD_IP -p $RAY_HEAD_PORT INFO | grep connected
fi

# Clean up any existing Ray processes
echo "Cleaning up existing Ray processes..."
ray stop || true
pkill -9 ray || true
pkill -9 plasma || true
pkill -9 raylet || true
rm -rf /tmp/ray/* || true

# Create log directory and enable core dumps
mkdir -p /tmp/ray/session_latest/logs /tmp/ray/debug
chmod 777 -R /tmp/ray
ulimit -c unlimited
echo "/tmp/ray/debug/core.%e.%p" > /proc/sys/kernel/core_pattern

# Start Ray worker with debug flags
echo "Starting Ray worker..."
RAYLET_DEBUG=1 ray start \
    --verbose \
    --address="$RAY_HEAD_IP:$RAY_HEAD_PORT" \
    --num-cpus="$NUM_CPUS" \
    --object-store-memory="$OBJECT_STORE_MEMORY" \
    --node-ip-address="$NODE_IP_ADDRESS" \
    --log-style=auto \
    --block &

RAY_PID=$!

# Monitor the Ray process
while true; do
    if ! ps -p $RAY_PID > /dev/null; then
        echo "Ray process died. Gathering debug information..."
        
        echo "=== Environment Variables ==="
        env | grep -i ray
        
        echo "=== Core Dumps ==="
        ls -l /tmp/ray/debug/
        
        echo "=== Process Status Before Exit ==="
        ps auxf
        
        echo "=== Ray Logs ==="
        for logfile in $(find /tmp/ray/session_latest/logs -type f); do
            echo "=== Contents of $logfile ==="
            echo "Last 100 lines of $logfile:"
            tail -n 100 "$logfile"
            
            echo "Checking for errors in $logfile:"
            grep -i "error\|failed\|crash\|exception" "$logfile" || true
        done
        
        echo "=== Network Status ==="
        echo "Connections:"
        netstat -anp | grep -E "$RAY_HEAD_IP|$NODE_IP_ADDRESS"
        
        echo "=== System Resources ==="
        free -h
        df -h
        
        exit 1
    fi
    sleep 5
done
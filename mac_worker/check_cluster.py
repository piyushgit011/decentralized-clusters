import ray
import sys
import time
import os

def check_head_node():
    try:
        head_ip = os.environ.get('RAY_HEAD_IP', 'localhost')
        head_port = int(os.environ.get('RAY_HEAD_PORT', '6379'))
        
        # Try to connect to the head node
        address = f"ray://{head_ip}:10001"
        print(f"Attempting to connect to Ray cluster at {address}")
        
        ray.init(
            address=address,
            log_to_driver=True,
            logging_level="debug"
        )
        
        # Check cluster info
        print("\nCluster Resources:")
        print(ray.cluster_resources())
        
        print("\nCluster Nodes:")
        print(ray.nodes())
        
        # Run a simple test
        @ray.remote
        def hello():
            return "Hello from worker!"
        
        result = ray.get(hello.remote())
        print(f"\nTest Result: {result}")
        
        ray.shutdown()
        return True
        
    except Exception as e:
        print(f"Error checking cluster: {str(e)}", file=sys.stderr)
        return False

if __name__ == "__main__":
    success = check_head_node()
    sys.exit(0 if success else 1)
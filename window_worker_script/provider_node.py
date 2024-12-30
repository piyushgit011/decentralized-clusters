import os
import time
import psutil
import sys
import ray
import logging
import nvidia_smi

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class RayProviderNode:
    def __init__(self):
        self.head_node = os.environ.get('RAY_HEAD_NODE', '67.205.167.215')
        self.max_retries = 3
        self.retry_delay = 10

    def get_system_info(self):
        try:
            nvidia_smi.nvmlInit()
            device_count = nvidia_smi.nvmlDeviceGetCount()
            gpu_info = []
            for i in range(device_count):
                handle = nvidia_smi.nvmlDeviceGetHandleByIndex(i)
                info = nvidia_smi.nvmlDeviceGetMemoryInfo(handle)
                name = nvidia_smi.nvmlDeviceGetName(handle)
                gpu_info.append({
                    'name': name.decode('utf-8') if isinstance(name, bytes) else name,
                    'memory': info.total // (1024*1024)
                })
            logger.info(f"Found {device_count} GPUs: {gpu_info}")
        except Exception as e:
            logger.warning(f"No GPUs found or error detecting GPUs: {str(e)}")
            gpu_info = []
            device_count = 0

        system_info = {
            'cpu_count': psutil.cpu_count(),
            'memory': psutil.virtual_memory().total,
            'gpu_count': device_count,
            'gpu_info': gpu_info
        }
        logger.info(f"System information: {system_info}")
        return system_info

    def start_ray_worker(self):
     for attempt in range(self.max_retries):
        try:
            logger.info(f"Attempt {attempt + 1} to connect to Ray cluster at {self.head_node}")
            
            # Ensure Ray is not running
            if ray.is_initialized():
                ray.shutdown()

            # Get GPU count
            try:
                nvidia_smi.nvmlInit()
                num_gpus = nvidia_smi.nvmlDeviceGetCount()
            except:
                num_gpus = 0

            # Connect to the Ray cluster as a worker node with GPU resources
            ray.init(
                address=f"ray://{self.head_node}:10001",
                ignore_reinit_error=True,
                resources={
                    "GPU": num_gpus,
                    "CPU": psutil.cpu_count(),
                },
                runtime_env={
                    "pip": ["torch", "numpy"]
                }
            )
            
            logger.info(f"Successfully connected to Ray cluster with {num_gpus} GPUs")
            return True

        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed: {str(e)}")
            if attempt < self.max_retries - 1:
                logger.info(f"Retrying in {self.retry_delay} seconds...")
                time.sleep(self.retry_delay)
            else:
                logger.error("Max retries reached. Failed to connect to Ray cluster.")
                return False

    def run(self):
        # Get and display system information
        self.get_system_info()

        logger.info("Starting Ray worker node...")
        if not self.start_ray_worker():
            sys.exit(1)

        logger.info("Ray worker node is running. Press Ctrl+C to stop.")
        try:
            while True:
                if ray.is_initialized():
                    try:
                        resources = ray.available_resources()
                        logger.info(f"Available resources: {resources}")
                    except Exception as e:
                        logger.error(f"Error getting resources: {str(e)}")
                        if not self.start_ray_worker():
                            break
                time.sleep(60)
        except KeyboardInterrupt:
            logger.info("\nShutting down...")
            ray.shutdown()
            logger.info("Shutdown complete")
        except Exception as e:
            logger.error(f"Error: {str(e)}")
            ray.shutdown()
            sys.exit(1)

def main():
    logger.info("Starting Ray worker node...")
    provider_node = RayProviderNode()
    provider_node.run()

if __name__ == "__main__":
    main()
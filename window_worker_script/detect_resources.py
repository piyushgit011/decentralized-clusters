# detect_resources.py is a script that detects the resources available on the machine where it is run.
import psutil
import GPUtil
import json

def detect_resources():
    cpu_count = psutil.cpu_count()
    memory = psutil.virtual_memory().total
    gpus = GPUtil.getGPUs()
    
    resources = {
        'cpu_count': cpu_count,
        'memory_bytes': memory,
        'gpu_count': len(gpus),
        'gpus': [{'id': gpu.id, 'memory_total': gpu.memoryTotal * 1024 * 1024} for gpu in gpus]  # Convert MB to bytes
    }
    
    return resources

if __name__ == "__main__":
    resources = detect_resources()
    print(json.dumps(resources))
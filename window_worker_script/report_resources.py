import psutil
import GPUtil
import requests
import os
import json

def report_resources():
    provider_id = os.environ['PROVIDER_ID']
    
    cpu_count = psutil.cpu_count()
    memory = psutil.virtual_memory().total
    gpus = GPUtil.getGPUs()
    
    resources = {
        'provider_id': provider_id,
        'cpu_count': cpu_count,
        'memory': memory,
        'gpu_count': len(gpus),
        'gpu_info': [{'name': gpu.name, 'memory': gpu.memoryTotal} for gpu in gpus]
    }
    
    try:
        response = requests.post('https://67.205.167.215:8000/report', 
                                 json=resources, 
                                 verify=False)  # Note: Use verify=False only for testing. In production, use proper SSL verification.
        response.raise_for_status()
        print("Resource report submitted successfully")
    except requests.exceptions.RequestException as e:
        print(f"Failed to submit resource report: {e}")

if __name__ == "__main__":
    report_resources()
import requests
import time
import logging
import argparse

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GPUClusterClient:
    def __init__(self, master_url):
        self.master_url = master_url

    def submit_task(self, task_type, data):
        task = {
            "type": task_type,
            "data": data
        }
        logger.info(f"Submitting {task_type} task")
        response = requests.post(f"{self.master_url}/submit_task", json=task)
        response.raise_for_status()
        job_id = response.json()["job_id"]
        logger.info(f"Task submitted successfully. Job ID: {job_id}")
        return job_id

    def get_job_status(self, job_id):
        logger.debug(f"Checking status for job: {job_id}")
        response = requests.get(f"{self.master_url}/job_status/{job_id}")
        response.raise_for_status()
        return response.json()

    def wait_for_job(self, job_id, polling_interval=5):
        while True:
            status = self.get_job_status(job_id)
            logger.info(f"Job {job_id} status: {status['status']}")
            if status["status"] in ["completed", "failed"]:
                return status
            time.sleep(polling_interval)

def train_model(client):
    training_config = {
        "input_size": 10,
        "hidden_size": 50,
        "output_size": 1,
        "lr": 0.001,
        "epochs": 10
    }
    job_id = client.submit_task("training", training_config)
    return job_id

def run_inference(client):
    inference_input = [[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]]
    job_id = client.submit_task("inference", {"input": inference_input})
    return job_id

def main():
    parser = argparse.ArgumentParser(description="GPU Cluster Client")
    parser.add_argument("--master_url", default="http://localhost:8000", help="URL of the master node")
    parser.add_argument("--action", choices=["train", "inference", "status"], required=True, help="Action to perform")
    parser.add_argument("--job_id", help="Job ID for checking status")
    args = parser.parse_args()

    client = GPUClusterClient(args.master_url)

    if args.action == "train":
        job_id = train_model(client)
        print(f"Training job submitted. Job ID: {job_id}")
    elif args.action == "inference":
        job_id = run_inference(client)
        print(f"Inference job submitted. Job ID: {job_id}")
    elif args.action == "status":
        if not args.job_id:
            print("Error: --job_id is required for checking status")
            return
        status = client.get_job_status(args.job_id)
        print(f"Job status: {status}")
        if status["status"] == "completed":
            print(f"Result: {status['result']}")

if __name__ == "__main__":
    main()
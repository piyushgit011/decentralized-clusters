# main.py
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import uuid
import subprocess
import time
import yaml
import os
from typing import Dict
import asyncio
import logging
from logging.handlers import RotatingFileHandler
from typing import Dict, Optional
from cachetools import TTLCache
from time import time as current_time

cluster_cache = TTLCache(maxsize=1000, ttl=5)
# Set up logging
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log_file = 'ray_cluster_service.log'
log_handler = RotatingFileHandler(log_file, maxBytes=1024 * 1024 * 100, backupCount=20)
log_handler.setFormatter(log_formatter)
logger = logging.getLogger('ray_cluster_service')
logger.setLevel(logging.DEBUG)
logger.addHandler(log_handler)

app = FastAPI()

# Store active clusters
active_clusters: Dict[str, Dict] = {}

class ClusterRequest(BaseModel):
    num_workers: int
    duration: int  # in minutes

class ClusterInfo(BaseModel):
    cluster_id: str
    status: str
    head_node_ip: Optional[str] = None
    expiration_time: Optional[float] = None
    error: Optional[str] = None
    creation_time: float
    num_workers: int

class ClusterResponse(BaseModel):
    cluster_id: str

# Load the base configuration
with open('config.yaml', 'r') as f:
    BASE_CONFIG = yaml.safe_load(f)

def generate_cluster_config(cluster_id: str, num_workers: int):
    config = BASE_CONFIG.copy()
    config["cluster_name"] = f"user-cluster-{cluster_id}"
    config["min_workers"] = num_workers
    config["max_workers"] = num_workers
    config["available_node_types"]["ray_worker_default"]["min_workers"] = num_workers
    config["available_node_types"]["ray_worker_default"]["max_workers"] = num_workers
    return config

async def run_command(command):
    logger.info(f"Running command: {command}")
    process = await asyncio.create_subprocess_shell(
        command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await process.communicate()
    if process.returncode != 0:
        error_msg = f"Command failed: {stderr.decode()}"
        logger.error(error_msg)
        raise Exception(error_msg)
    logger.debug(f"Command output: {stdout.decode().strip()}")
    return stdout.decode().strip()

async def deploy_cluster(cluster_id: str, num_workers: int):
    logger.info(f"Deploying cluster {cluster_id} with {num_workers} workers")
    config = generate_cluster_config(cluster_id, num_workers)
    config_path = f"/tmp/ray_config_{cluster_id}.yaml"
    with open(config_path, "w") as f:
        yaml.dump(config, f)
    
    try:
        await run_command(f"ray up -y {config_path}")
        head_node_ip = await run_command(f"ray get-head-ip {config_path}")
        head_node_ip = head_node_ip.strip()
        logger.info(f"Cluster {cluster_id} deployed successfully. Head node IP: {head_node_ip}")
        return head_node_ip
    except Exception as e:
        logger.error(f"Failed to deploy cluster {cluster_id}: {str(e)}")
        raise

async def terminate_cluster(cluster_id: str):
    logger.info(f"Terminating cluster {cluster_id}")
    config_path = f"/tmp/ray_config_{cluster_id}.yaml"
    if not os.path.exists(config_path):
        error_msg = f"Config file for cluster {cluster_id} not found"
        logger.error(error_msg)
        raise Exception(error_msg)
    
    try:
        await run_command(f"ray down -y {config_path}")
        os.remove(config_path)
        logger.info(f"Cluster {cluster_id} terminated successfully")
    except Exception as e:
        logger.error(f"Failed to terminate cluster {cluster_id}: {str(e)}")
        raise

async def setup_cluster(cluster_id: str, num_workers: int, duration: int):
    logger.info(f"Setting up cluster {cluster_id}")
    try:
        active_clusters[cluster_id]["status"] = "deploying"
        
        # Run deploy_cluster in a separate task
        deploy_task = asyncio.create_task(deploy_cluster(cluster_id, num_workers))
        
        # Wait for deploy_cluster to complete
        head_node_ip = await deploy_task
        
        active_clusters[cluster_id].update({
            "status": "running",
            "head_node_ip": head_node_ip,
            "expiration_time": time.time() + duration * 60,
        })
        logger.info(f"Cluster {cluster_id} setup completed")
    except Exception as e:
        active_clusters[cluster_id]["status"] = "failed"
        active_clusters[cluster_id]["error"] = str(e)
        logger.error(f"Cluster {cluster_id} deployment failed: {str(e)}")

@app.post("/deploy_cluster", response_model=ClusterResponse)
async def create_cluster(request: ClusterRequest, background_tasks: BackgroundTasks):
    cluster_id = str(uuid.uuid4())
    logger.info(f"Received request to create cluster {cluster_id}")
    
    creation_time = time.time()
    cluster_info = {
        "cluster_id": cluster_id,
        "status": "initializing",
        "num_workers": request.num_workers,
        "creation_time": creation_time,
        "expiration_time": creation_time + request.duration * 60,
    }
    active_clusters[cluster_id] = cluster_info
    
    # Start the setup process in the background
    asyncio.create_task(setup_cluster(cluster_id, request.num_workers, request.duration))
    
    logger.info(f"Cluster {cluster_id} creation initiated")
    return ClusterResponse(cluster_id=cluster_id)

def get_cached_cluster_data(cluster_id: str) -> Optional[Dict]:
    current_timestamp = current_time()  # Use current_time() instead of time()
    cache_key = f"{cluster_id}_{current_timestamp // 5}"  # Create a new cache entry every 5 seconds
    
    if cache_key not in cluster_cache:
        if cluster_id not in active_clusters:
            return None
        cluster_data = active_clusters[cluster_id].copy()
        cluster_cache[cache_key] = {
            "cluster_id": cluster_data["cluster_id"],
            "status": cluster_data["status"],
            "num_workers": cluster_data["num_workers"],
            "creation_time": cluster_data["creation_time"],
            "expiration_time": cluster_data.get("expiration_time"),
            "head_node_ip": cluster_data.get("head_node_ip"),
            "error": cluster_data.get("error")
        }
    
    return cluster_cache[cache_key]

@app.get("/cluster/{cluster_id}", response_model=ClusterInfo)
def get_cluster_info(cluster_id: str):
    logger.info(f"Received request for cluster info: {cluster_id}")
    
    cluster_data = get_cached_cluster_data(cluster_id)
    
    if cluster_data is None:
        logger.warning(f"Cluster {cluster_id} not found")
        raise HTTPException(status_code=404, detail="Cluster not found")
    
    logger.info(f"Returning info for cluster {cluster_id}")
    return ClusterInfo(**cluster_data)

@app.delete("/cluster/{cluster_id}")
async def delete_cluster(cluster_id: str):
    logger.info(f"Received request to delete cluster {cluster_id}")
    if cluster_id not in active_clusters:
        logger.warning(f"Cluster {cluster_id} not found")
        raise HTTPException(status_code=404, detail="Cluster not found")
    
    try:
        await terminate_cluster(cluster_id)
    except Exception as e:
        logger.error(f"Failed to delete cluster {cluster_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    
    del active_clusters[cluster_id]
    logger.info(f"Cluster {cluster_id} deleted successfully")
    return {"message": "Cluster terminated successfully"}

async def check_and_terminate_expired_clusters():
    while True:
        logger.debug("Checking for expired clusters")
        current_time = time.time()
        clusters_to_terminate = [
            cluster_id for cluster_id, info in active_clusters.items()
            if info.get("expiration_time") and info["expiration_time"] <= current_time
        ]
        
        for cluster_id in clusters_to_terminate:
            logger.info(f"Terminating expired cluster: {cluster_id}")
            try:
                await terminate_cluster(cluster_id)
                del active_clusters[cluster_id]
                logger.info(f"Expired cluster {cluster_id} terminated successfully")
            except Exception as e:
                logger.error(f"Failed to terminate expired cluster {cluster_id}: {str(e)}")
        
        await asyncio.sleep(60)  # Check every minute

@app.on_event("startup")
async def startup_event():
    logger.info("Starting Ray Cluster Service")
    asyncio.create_task(check_and_terminate_expired_clusters())

if __name__ == "__main__":
    import uvicorn
    logger.info("Starting FastAPI server")
    uvicorn.run(app, host="0.0.0.0", port=8000)

# File ends here
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx
import asyncio
import ray
from typing import Dict, Any
import uuid
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = FastAPI()

# Initialize Ray
ray.init(address="auto", namespace="gpu_cluster")

class Node(BaseModel):
    node_id: str
    address: str

class Task(BaseModel):
    type: str
    data: dict

class JobStatus(BaseModel):
    status: str
    result: Dict[str, Any] = None

nodes = {}
job_statuses: Dict[str, JobStatus] = {}

@ray.remote
class JobQueue:
    def __init__(self):
        self.queue = asyncio.Queue()

    async def enqueue(self, item):
        await self.queue.put(item)

    async def dequeue(self):
        return await self.queue.get()

    def empty(self):
        return self.queue.empty()

job_queue = JobQueue.remote()

@app.post("/register_node")
async def register_node(node: Node):
    nodes[node.node_id] = node
    logger.info(f"Node registered: {node.node_id} at {node.address}")
    return {"message": "Node registered successfully"}

@app.post("/submit_task")
async def submit_task(task: Task):
    job_id = str(uuid.uuid4())
    await job_queue.enqueue.remote((job_id, task))
    job_statuses[job_id] = JobStatus(status="queued")
    return {"job_id": job_id}

@app.get("/job_status/{job_id}")
async def get_job_status(job_id: str):
    if job_id not in job_statuses:
        raise HTTPException(status_code=404, detail="Job not found")
    return job_statuses[job_id]

async def process_queue():
    while True:
        if not await job_queue.empty.remote():
            job_id, task = await job_queue.dequeue.remote()
            job_statuses[job_id].status = "processing"
            
            if not nodes:
                job_statuses[job_id].status = "failed"
                job_statuses[job_id].result = {"error": "No worker nodes available"}
                continue
            
            node_id = list(nodes.keys())[len(nodes) % len(nodes)]
            node = nodes[node_id]
            
            try:
                async with httpx.AsyncClient() as client:
                    response = await client.post(f"http://{node.address}/execute_task", json=task.dict())
                result = response.json()
                job_statuses[job_id].status = "completed"
                job_statuses[job_id].result = result
            except Exception as e:
                logger.error(f"Error processing task: {str(e)}")
                job_statuses[job_id].status = "failed"
                job_statuses[job_id].result = {"error": str(e)}
        
        await asyncio.sleep(1)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(process_queue())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
import os
import uuid
import logging
from fastapi import FastAPI, HTTPException
import uvicorn
import httpx
import asyncio
import ray
import torch
import torch.nn as nn
import torch.optim as optim
from pydantic import BaseModel

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = FastAPI()

# Initialize Ray
ray.init(address="auto", namespace="gpu_cluster")

NODE_ID = str(uuid.uuid4())
MASTER_URL = os.environ.get("MASTER_URL", "http://localhost:8000")
NODE_PORT = int(os.environ.get("NODE_PORT", 8001))

class Task(BaseModel):
    type: str
    data: dict

class SimpleModel(nn.Module):
    def __init__(self, input_size, hidden_size, output_size):
        super(SimpleModel, self).__init__()
        self.layer1 = nn.Linear(input_size, hidden_size)
        self.layer2 = nn.Linear(hidden_size, output_size)

    def forward(self, x):
        x = torch.relu(self.layer1(x))
        return self.layer2(x)

@ray.remote
def train_model(config):
    logger.debug(f"Starting training with config: {config}")
    model = SimpleModel(config["input_size"], config["hidden_size"], config["output_size"])
    optimizer = optim.Adam(model.parameters(), lr=config["lr"])
    criterion = nn.MSELoss()

    X = torch.randn(1000, config["input_size"])
    y = torch.randn(1000, config["output_size"])

    for epoch in range(config["epochs"]):
        optimizer.zero_grad()
        outputs = model(X)
        loss = criterion(outputs, y)
        loss.backward()
        optimizer.step()
        logger.debug(f"Epoch {epoch+1}/{config['epochs']}, Loss: {loss.item()}")

    torch.save(model.state_dict(), "model.pth")
    logger.debug("Model trained and saved")
    return {"message": "Model trained and saved"}

@ray.remote
def run_inference(input_data):
    logger.debug(f"Running inference with input shape: {input_data.shape}")
    try:
        model = SimpleModel(input_data.shape[1], 50, 1)
        model.load_state_dict(torch.load("model.pth"))
        model.eval()
        with torch.no_grad():
            output = model(input_data).numpy().tolist()
        logger.debug(f"Inference output: {output}")
        return output
    except Exception as e:
        logger.error(f"Error during inference: {str(e)}")
        raise

@app.post("/execute_task")
async def execute_task(task: Task):
    logger.info(f"Received task: {task.type}")
    try:
        if task.type == "training":
            result = ray.get(train_model.remote(task.data))
        elif task.type == "inference":
            input_data = torch.tensor(task.data["input"], dtype=torch.float32)
            result = ray.get(run_inference.remote(input_data))
        else:
            raise ValueError("Invalid task type")

        logger.info(f"Task completed successfully: {task.type}")
        return {"result": result}
    except Exception as e:
        logger.error(f"Error executing task: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.on_event("startup")
async def startup_event():
    logger.info(f"Worker node starting up. ID: {NODE_ID}, Port: {NODE_PORT}")
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(f"{MASTER_URL}/register_node", json={"node_id": NODE_ID, "address": f"localhost:{NODE_PORT}"})
            response.raise_for_status()
            logger.info("Successfully registered with master node")
        except Exception as e:
            logger.error(f"Failed to register with master node: {str(e)}")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=NODE_PORT)
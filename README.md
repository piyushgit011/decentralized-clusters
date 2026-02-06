

Let me start by understanding the repository structure and codebase before creating comprehensive documentation.Let me first explore the repository structure and understand how Ray and the decentralized cluster are implemented.Now let me read all the key files in the repository:Now let me also fetch the subdirectories:Now let me get the worker node files:I've now thoroughly analyzed the entire codebase. Here is the complete documentation for your **Decentralized GPU Cluster** project:

---

# 📖 Decentralized GPU Cluster — Complete Documentation

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [How Ray Is Used](#3-how-ray-is-used)
4. [Repository Structure](#4-repository-structure)
5. [Component Deep Dives](#5-component-deep-dives)
6. [Provider (GPU Owner) Flow](#6-provider-gpu-owner-flow)
7. [Buyer (Cluster Consumer) Flow](#7-buyer-cluster-consumer-flow)
8. [Deployment & Infrastructure](#8-deployment--infrastructure)
9. [API Reference](#9-api-reference)
10. [Ray Commands Quick Reference](#10-ray-commands-quick-reference)
11. [Troubleshooting](#11-troubleshooting)

---

## 1. Project Overview

This project implements a **decentralized GPU cluster marketplace** where:

- **GPU Providers** (anyone with a GPU) can register and contribute their compute resources to a shared Ray cluster.
- **Buyers** can request on-demand GPU clusters via a REST API, use them for training/inference, and have them automatically expire after a set duration.

The system is built on top of **[Ray](https://www.ray.io/)** — an open-source distributed computing framework — combined with **FastAPI** for the management API, **Docker** for containerized worker deployment, and **AWS** for cloud-based head/worker node provisioning.

### Key Technologies

| Technology | Role |
|---|---|
| **Ray** | Distributed computing framework — manages the cluster, schedules tasks across GPU nodes |
| **FastAPI** | REST API for cluster lifecycle management and provider registration |
| **Docker** | Containerized worker nodes that can run on any machine (Linux, macOS, Windows) |
| **AWS (EC2)** | Cloud infrastructure provider for head nodes and optional cloud GPU workers |
| **SQLite + JWT** | Provider authentication and registration |

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        PLATFORM LAYER                           │
│                                                                 │
│  ┌──────────────────┐    ┌──────────────────────────────────┐   │
│  │  Provider         │    │  Cluster Management API          │   │
│  │  Registration     │    │  (main.py - FastAPI :8000)       │   │
│  │  (FastAPI :8000)  │    │                                  │   │
│  │  • /register      │    │  • POST /deploy_cluster          │   │
│  │  • /token         │    │  • GET  /cluster/{id}            │   │
│  │  • /providers/me  │    │  • DELETE /cluster/{id}          │   │
│  └──────────────────┘    │  • Auto-expiry background task   │   │
│                          └──────────┬───────────────────────┘   │
���                                     │                           │
│                                     │ ray up / ray down         │
│                                     ▼                           │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                    RAY CLUSTER                            │   │
│  │                                                          │   │
│  │  ┌──────────────┐                                        │   │
│  │  │  HEAD NODE    │  (t3.xlarge on AWS)                    │   │
│  │  │  • GCS        │  Port 6379 (Ray), 8265 (Dashboard)    │   │
│  │  │  • Scheduler  │  Port 10001 (Ray Client)              │   │
│  │  │  • Dashboard  │                                        │   │
│  │  └──────┬───────┘                                        │   │
│  │         │                                                │   │
│  │    ┌────┴─────┬──────────┬──────────┐                    │   │
│  │    ▼          ▼          ▼          ▼                    │   │
│  │ ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐            │   │
│  │ │Worker 1│ │Worker 2│ │Worker 3│ │Worker N│            │   │
│  │ │(GPU)   │ │(GPU)   │ │(GPU)   │ │(GPU)   │            │   │
│  │ │AWS EC2 │ │Docker  │ │Docker  │ │Docker  │            │   │
│  │ │g4dn.xl │ │Linux   │ │macOS   │ │Windows │            │   │
│  │ └────────┘ └────────┘ └────────┘ └────────┘            │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                    BUYER / CLIENT                         │   │
│  │  client.py → ray.init(address="ray://HEAD_IP:10001")     │   │
│  │  • Submit training jobs (@ray.remote)                    │   │
│  │  • Submit inference jobs (@ray.remote)                   │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. How Ray Is Used

### 3.1 Ray as the Distributed Computing Backbone

Ray is the core technology that enables decentralized GPU clustering. Here's how each Ray feature is leveraged:

#### a) **Ray Cluster Formation (Head + Workers)**

The system uses Ray's **cluster launcher** (`ray up` / `ray down`) to dynamically provision and manage clusters. The head node acts as the central coordinator:

```python name=main.py url=https://github.com/piyushgit011/decentralized-clusters/blob/0f4fc66dd37de714e0509b15835d2ea3fc35ae4f/main.py#L76-L91
async def deploy_cluster(cluster_id: str, num_workers: int):
    config = generate_cluster_config(cluster_id, num_workers)
    config_path = f"/tmp/ray_config_{cluster_id}.yaml"
    with open(config_path, "w") as f:
        yaml.dump(config, f)
    
    try:
        await run_command(f"ray up -y {config_path}")
        head_node_ip = await run_command(f"ray get-head-ip {config_path}")
        head_node_ip = head_node_ip.strip()
        return head_node_ip
    except Exception as e:
        raise
```

- **`ray up`** — Provisions the head node and worker nodes on AWS, installs Ray, and starts the cluster.
- **`ray get-head-ip`** — Retrieves the head node's IP so buyers can connect.
- **`ray down`** — Tears down the cluster when expired or deleted.

#### b) **Dynamic Cluster Configuration**

Each buyer gets an isolated cluster with a unique configuration generated from a base template:

```python name=main.py url=https://github.com/piyushgit011/decentralized-clusters/blob/0f4fc66dd37de714e0509b15835d2ea3fc35ae4f/main.py#L52-L59
def generate_cluster_config(cluster_id: str, num_workers: int):
    config = BASE_CONFIG.copy()
    config["cluster_name"] = f"user-cluster-{cluster_id}"
    config["min_workers"] = num_workers
    config["max_workers"] = num_workers
    config["available_node_types"]["ray_worker_default"]["min_workers"] = num_workers
    config["available_node_types"]["ray_worker_default"]["max_workers"] = num_workers
    return config
```

#### c) **Ray Head Node Configuration**

The head node starts Ray with these critical parameters:

```yaml name=config.yaml url=https://github.com/piyushgit011/decentralized-clusters/blob/0f4fc66dd37de714e0509b15835d2ea3fc35ae4f/config.yaml#L71-L82
head_start_ray_commands:
  - bash -c 'source ~/setup_ray_env.sh && ray stop'
  - >
    bash -c '
    source ~/setup_ray_env.sh &&
    ulimit -n 65536 &&
    export RAY_HEAD_EXTERNAL_IP=$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4) &&
    export RAY_HEAD_INTERNAL_IP=$(hostname -I | awk "{print \$1}") &&
    ray start --head --port=6379 --object-manager-port=8076 --dashboard-host=0.0.0.0 
    --node-ip-address=$RAY_HEAD_INTERNAL_IP --redis-password="password" 
    --autoscaling-config=~/ray_bootstrap_config.yaml
    '
```

| Port | Purpose |
|------|---------|
| `6379` | Ray GCS (Global Control Store) — worker nodes connect here |
| `8076` | Object Manager — shared object transfer between nodes |
| `8265` | Ray Dashboard — monitoring UI |
| `10001` | Ray Client — buyers connect here to submit jobs |

#### d) **Ray Worker Nodes Joining the Cluster**

Workers connect to the head node via the GCS port:

```yaml name=config.yaml url=https://github.com/piyushgit011/decentralized-clusters/blob/0f4fc66dd37de714e0509b15835d2ea3fc35ae4f/config.yaml#L84-L93
worker_start_ray_commands:
  - bash -c 'source ~/setup_ray_env.sh && ray stop'
  - >
    bash -c '
    source ~/setup_ray_env.sh &&
    ulimit -n 65536 &&
    export WORKER_INTERNAL_IP=$(hostname -I | awk "{print \$1}") &&
    ray start --address=$RAY_HEAD_IP:6379 --object-manager-port=8076
    --node-ip-address=$WORKER_INTERNAL_IP --redis-password="password"
    '
```

#### e) **Ray Remote Functions (Distributed Task Execution)**

Buyers submit work using Ray's `@ray.remote` decorator. Ray automatically schedules tasks across available GPU workers:

```python name=client.py url=https://github.com/piyushgit011/decentralized-clusters/blob/0f4fc66dd37de714e0509b15835d2ea3fc35ae4f/client.py#L7-L18
@ray.remote
def train_model(data: list, epochs: int = 10) -> list:
    model = np.array(data)
    for _ in range(epochs):
        model += np.random.rand(len(model)) * 0.1
    return model.tolist()

@ray.remote
def run_inference(model: list, input_data: list) -> float:
    return np.dot(np.array(model), np.array(input_data))
```

Worker nodes also define Ray remote functions for more complex ML tasks using PyTorch:

```python name=worker/worker-node.py url=https://github.com/piyushgit011/decentralized-clusters/blob/0f4fc66dd37de714e0509b15835d2ea3fc35ae4f/worker/worker-node.py#L41-L61
@ray.remote
def train_model(config):
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
    torch.save(model.state_dict(), "model.pth")
    return {"message": "Model trained and saved"}
```

#### f) **Ray Autoscaler**

The cluster configuration enables Ray's built-in autoscaler:

```yaml name=ray-cluster-config.yaml url=https://github.com/piyushgit011/decentralized-clusters/blob/0f4fc66dd37de714e0509b15835d2ea3fc35ae4f/ray-cluster-config.yaml#L1-L6
cluster_name: dynamic-gpu-cluster
min_workers: 1
max_workers: 2
upscaling_speed: 1.0
```

This means Ray automatically scales GPU workers between `min_workers` and `max_workers` based on workload demand.

---

## 4. Repository Structure

```
decentralized-clusters/
├���─ main.py                          # 🎯 Core API: Cluster lifecycle management
├── client.py                        # 🛒 Buyer client: Connect & submit jobs
├── config.yaml                      # ⚙️ Base Ray cluster config (AWS)
├── ray-cluster-config.yaml          # ⚙️ Alternative Ray cluster config
├── docker-compose.yml               # 🐳 Local dev: Head + Master + Worker
├── requirements.txt                 # 📦 Python dependencies
├── worker.sh                        # 🔧 Linux worker join script
├── ray_worker.bat                   # 🔧 Windows worker join script
├── ray-commands.txt                 # 📝 Useful Ray CLI commands
├── new.json                         # 📝 Base config in JSON format
│
├── master/                          # 🏗️ Master node Docker container
│   ├── Dockerfile
│   └── master-node.py
│
├── worker/                          # 🏗️ Worker node Docker container
│   ├── Dockerfile
│   ├── worker-node.py               # Worker with PyTorch training/inference
│   ├── authenticate.py              # Provider auth (same as providers_registration)
│   └── .env
│
├── mac_worker/                      # 🍎 macOS ARM64 worker container
│   ├── Dockerfile                   # ARM64 Python image + Ray 2.37.0
│   ├── start-worker.sh              # Worker startup with monitoring
│   └── check_cluster.py             # Cluster connectivity checker
│
├── window_worker_script/            # 🪟 Windows/NVIDIA GPU worker container
│   ├── Dockerfile                   # NVIDIA CUDA base + Ray 2.37.0
│   ├── provider_node.py             # GPU-aware provider node with nvidia-smi
│   ├── authenticate.py              # JWT auth verification
│   ├── detect_resources.py          # CPU/GPU/Memory detection
│   ├── report_resources.py          # Report resources to master
│   ├── test_gpu.py                  # PyTorch GPU validation
│   ��── entrypoint.sh                # Container entrypoint
│   └── final.sh                     # Docker run script
│
└── providers_registration/          # 🔐 Provider auth service
    └── main.py                      # FastAPI: register, login, JWT tokens
```

---

## 5. Component Deep Dives

### 5.1 Cluster Management API (`main.py`)

This is the central service that buyers interact with. It manages the full lifecycle of Ray clusters.

**Key Features:**
- **Cluster Creation** — Generates a unique cluster config, runs `ray up` in the background
- **Cluster Monitoring** — TTL-cached status checks (5-second cache)
- **Cluster Deletion** — Runs `ray down` and cleans up config files
- **Auto-Expiry** — Background task checks every 60 seconds for expired clusters

**Cluster Lifecycle States:**

```
initializing → deploying → running → [expired/deleted]
                         → failed
```

### 5.2 Provider Registration Service (`providers_registration/main.py`)

A standalone FastAPI service for GPU provider identity management:

```python name=providers_registration/main.py url=https://github.com/piyushgit011/decentralized-clusters/blob/0f4fc66dd37de714e0509b15835d2ea3fc35ae4f/providers_registration/main.py#L76-L89
@app.post("/register", response_model=Provider)
async def register_provider(provider: ProviderCreate):
    # Check for duplicate username
    # Hash password with bcrypt
    # Store in SQLite database
    # Return provider details
```

| Feature | Implementation |
|---------|---------------|
| Database | SQLite (`providers.db`) |
| Password Hashing | bcrypt via `passlib` |
| Authentication | JWT (HS256) via `pyjwt` |
| Token Expiry | 30 minutes |

### 5.3 Worker Node (`worker/worker-node.py`)

The worker node is a FastAPI + Ray hybrid that:
1. Initializes Ray (`ray.init(address="auto", namespace="gpu_cluster")`)
2. Registers itself with the master node via HTTP
3. Exposes a `/execute_task` endpoint for training and inference
4. Uses `@ray.remote` decorated functions that Ray schedules across the cluster

### 5.4 Platform-Specific Worker Containers

| Platform | Directory | Base Image | GPU Support |
|----------|-----------|------------|-------------|
| **Linux/Generic** | `worker/` | `python:3.9-slim` | Via Docker `--gpus all` |
| **macOS (ARM64)** | `mac_worker/` | `python:3.12-slim` (ARM64) | CPU-only (Apple Silicon) |
| **Windows/NVIDIA** | `window_worker_script/` | `nvidia/cuda:12.0.0-base-ubuntu20.04` | Full CUDA 12.0 + PyTorch |

### 5.5 Docker Compose (Local Development)

```yaml name=docker-compose.yml url=https://github.com/piyushgit011/decentralized-clusters/blob/0f4fc66dd37de714e0509b15835d2ea3fc35ae4f/docker-compose.yml
version: '3'
services:
  ray-head:
    image: rayproject/ray:latest
    ports:
      - "6379:6379"
      - "8265:8265"
    command: ray start --head --port=6379 --dashboard-port=8265 --dashboard-host=0.0.0.0

  master:
    build:
      context: ./master
      dockerfile: Dockerfile
    ports:
      - "8000:8000"
    environment:
      - RAY_ADDRESS=ray://ray-head:10001
    depends_on:
      - ray-head

  worker:
    build:
      context: ./worker
      dockerfile: Dockerfile
    environment:
      - MASTER_URL=http://master:8000
      - NODE_PORT=8001
      - RAY_ADDRESS=ray://ray-head:10001
    depends_on:
      - master
      - ray-head
```

---

## 6. Provider (GPU Owner) Flow

### Step 1: Register as a Provider

```bash
curl -X POST http://67.205.167.215:8000/register \
  -H "Content-Type: application/json" \
  -d '{"username": "gpu_provider_1", "password": "securepassword"}'
```

### Step 2: Get an Authentication Token

```bash
curl -X POST http://67.205.167.215:8000/token \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=gpu_provider_1&password=securepassword"
```

### Step 3: Join the Cluster with Your GPU

**On Linux/macOS:**
```bash
./worker.sh <HEAD_NODE_IP> <PROVIDER_ID> <USERNAME> <PASSWORD>
```

**On Windows:**
```batch
ray_worker.bat <HEAD_NODE_IP> <PROVIDER_ID> <USERNAME> <PASSWORD>
```

**What happens under the hood (from `worker.sh`):**

```shell name=worker.sh url=https://github.com/piyushgit011/decentralized-clusters/blob/0f4fc66dd37de714e0509b15835d2ea3fc35ae4f/worker.sh#L56-L81
# 1. Pull the custom Ray Docker image
docker pull piyushaaryan/ioc:1.0

# 2. Run Ray worker container with GPU passthrough
docker run --rm \
    --name ray-worker-${PROVIDER_ID} \
    --network host \
    --shm-size=1gb \
    $GPU_FLAG \                                          # --gpus all if NVIDIA detected
    -e RAY_ADDRESS="ray://${HEAD_NODE_IP}:10001" \       # Connect to head node
    -e PROVIDER_ID="${PROVIDER_ID}" \
    -e AUTH_TOKEN="${AUTH_TOKEN}" \
    -e NVIDIA_VISIBLE_DEVICES=all \
    -e NVIDIA_DRIVER_CAPABILITIES=all \
    piyushaaryan/ioc:1.1
```

The script:
1. ✅ Checks Docker is installed
2. ✅ Detects NVIDIA GPU via `nvidia-smi`
3. ✅ Auto-installs NVIDIA Container Toolkit if needed
4. ✅ Authenticates with the provider registration service
5. ✅ Pulls the custom Ray Docker image
6. ✅ Starts a containerized Ray worker that joins the cluster

### Step 4: Resource Detection & Reporting

The **provider node** (`window_worker_script/provider_node.py`) automatically detects and reports GPU resources:

```python name=window_worker_script/provider_node.py url=https://github.com/piyushgit011/decentralized-clusters/blob/0f4fc66dd37de714e0509b15835d2ea3fc35ae4f/window_worker_script/provider_node.py#L67-L80
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
```

---

## 7. Buyer (Cluster Consumer) Flow

### Step 1: Request a Cluster

```bash
curl -X POST http://localhost:8000/deploy_cluster \
  -H "Content-Type: application/json" \
  -d '{"num_workers": 2, "duration": 60}'
```

**Response:**
```json
{"cluster_id": "550e8400-e29b-41d4-a716-446655440000"}
```

This triggers an async background process:
1. A unique YAML config is generated at `/tmp/ray_config_{cluster_id}.yaml`
2. `ray up -y` provisions the head node + N worker nodes on AWS
3. The cluster transitions: `initializing` → `deploying` → `running`

### Step 2: Check Cluster Status

```bash
curl http://localhost:8000/cluster/550e8400-e29b-41d4-a716-446655440000
```

**Response:**
```json
{
  "cluster_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "running",
  "head_node_ip": "54.227.33.106",
  "expiration_time": 1706216400.0,
  "creation_time": 1706212800.0,
  "num_workers": 2
}
```

### Step 3: Connect and Submit Jobs

Once the cluster is `running`, use the head node IP to submit distributed tasks:

```python name=client.py url=https://github.com/piyushgit011/decentralized-clusters/blob/0f4fc66dd37de714e0509b15835d2ea3fc35ae4f/client.py#L20-L38
def main():
    # Connect to the Ray cluster
    ray.init(address=f"ray://{HEAD_NODE_IP}:10001")

    try:
        # Example training
        data = [1.0, 2.0, 3.0, 4.0, 5.0]
        trained_model = ray.get(train_model.remote(data))
        print(f"Trained model: {trained_model}")

        # Example inference
        input_data = [0.5, 1.5, 2.5, 3.5, 4.5]
        result = ray.get(run_inference.remote(trained_model, input_data))
        print(f"Inference result: {result}")

    finally:
        ray.shutdown()
```

### Step 4: Cluster Auto-Expiry or Manual Deletion

Clusters automatically expire after the requested duration. The background task in `main.py` checks every 60 seconds:

```python name=main.py url=https://github.com/piyushgit011/decentralized-clusters/blob/0f4fc66dd37de714e0509b15835d2ea3fc35ae4f/main.py#L202-L220
async def check_and_terminate_expired_clusters():
    while True:
        current_time = time.time()
        clusters_to_terminate = [
            cluster_id for cluster_id, info in active_clusters.items()
            if info.get("expiration_time") and info["expiration_time"] <= current_time
        ]
        for cluster_id in clusters_to_terminate:
            await terminate_cluster(cluster_id)
            del active_clusters[cluster_id]
        await asyncio.sleep(60)
```

Or manually delete:
```bash
curl -X DELETE http://localhost:8000/cluster/550e8400-e29b-41d4-a716-446655440000
```

---

## 8. Deployment & Infrastructure

### AWS Node Configuration

| Node Type | Instance Type | Resources | Storage | Purpose |
|-----------|-------------|-----------|---------|---------|
| **Head Node** | `t3.xlarge` | 4 CPUs | 10 GB EBS | Cluster coordination, GCS, Dashboard |
| **GPU Worker** | `g4dn.xlarge` | 4 CPUs, 1 GPU (T4) | 100 GB EBS | ML training & inference workloads |

### Setup Commands (Auto-provisioned by Ray)

Each node is automatically configured with:
1. Python 3 virtual environment (`~/ray_env`)
2. Ray with default extensions
3. FastAPI + Uvicorn
4. File descriptor limit raised to 65536

### Security

- AWS Security Group: `ray-autoscaler-dynamic-gpu-cluster` — opens all TCP ports (0–65535) for inter-node communication
- SSH access: Ubuntu user with `ray-nodes.pem` key pair
- Provider auth: JWT tokens with 30-minute expiry, bcrypt password hashing

---

## 9. API Reference

### Cluster Management API (`main.py` — Port 8000)

| Method | Endpoint | Description | Request Body | Response |
|--------|----------|-------------|--------------|----------|
| `POST` | `/deploy_cluster` | Create a new Ray cluster | `{"num_workers": int, "duration": int}` | `{"cluster_id": "uuid"}` |
| `GET` | `/cluster/{cluster_id}` | Get cluster status and info | — | `ClusterInfo` object |
| `DELETE` | `/cluster/{cluster_id}` | Terminate and delete a cluster | — | `{"message": "Cluster terminated successfully"}` |

### Provider Registration API (`providers_registration/main.py` — Port 8000)

| Method | Endpoint | Description | Auth Required |
|--------|----------|-------------|---------------|
| `POST` | `/register` | Register a new GPU provider | No |
| `POST` | `/token` | Get JWT access token | No (credentials in body) |
| `GET` | `/providers/me` | Get current provider info | Yes (Bearer token) |

---

## 10. Ray Commands Quick Reference

```bash
# Add a worker node to the cluster
ray start --address='<HEAD_IP>:6379'

# Connect via Python
import ray
ray.init(address="ray://<HEAD_IP>:10001")

# Submit a job via CLI
RAY_ADDRESS='http://127.0.0.1:8265' ray job submit --working-dir . -- python my_script.py

# Cluster management
ray status                                    # View cluster status
ray stop                                      # Stop Ray on current node
ray down /path/to/config.yaml                 # Tear down entire cluster
ray get-head-ip /path/to/config.yaml          # Get head node IP
ray dashboard /path/to/config.yaml            # Port-forward dashboard
ray attach /path/to/config.yaml               # SSH into head node

# Monitor autoscaling
ray exec /path/to/config.yaml 'tail -n 100 -f /tmp/ray/session_latest/logs/monitor*'
```

---

## 11. Troubleshooting

| Issue | Solution |
|-------|----------|
| Worker can't connect to head node | Verify head node IP and port 6379/10001 are accessible. Check firewall rules. |
| No GPU detected in container | Ensure `nvidia-smi` works on host. Install NVIDIA Container Toolkit. Use `--gpus all` flag. |
| Cluster stuck in "deploying" | Check `ray_cluster_service.log` for errors. Verify AWS credentials and AMI availability. |
| Authentication failed | Verify username/password. Check if provider registration service is running. |
| Docker not installed | Install Docker Desktop (Windows/macOS) or Docker Engine (Linux). |
| Ray Dashboard not accessible | Port-forward with `ray dashboard config.yaml` or check port 8265 is open. |

**Log file:** All cluster management operations are logged to `ray_cluster_service.log` with rotation (100MB per file, 20 backups).

---

This documentation covers the complete system as implemented in your [`piyushgit011/decentralized-clusters`](https://github.com/piyushgit011/decentralized-clusters) repository. The architecture enables a true marketplace model where GPU providers contribute resources via Docker containers and buyers provision on-demand clusters through a simple REST API — all orchestrated by Ray's distributed computing framework.

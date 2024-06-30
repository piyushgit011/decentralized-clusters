# Decentralized GPU Cluster

This project implements a decentralized GPU cluster system using Ray, FastAPI, and Docker. It consists of a master node, worker nodes, and a client for submitting and monitoring jobs.

## File Structure

```
.
├── README.md               # This file
├── client.py               # Client script for submitting jobs and checking status
├── docker-compose.yml      # Docker Compose configuration file
├── requirements.txt        # Python dependencies for the project
├── master/
│   ├── Dockerfile          # Dockerfile for building the master node image
│   └── master-node.py      # Master node implementation
└── worker/
    ├── Dockerfile          # Dockerfile for building the worker node image
    └── worker-node.py      # Worker node implementation
```

## Setup Instructions

1. Ensure you have Docker and Docker Compose installed on your system.

2. Clone this repository:
   ```
   git clone [<repository-url>](https://github.com/piyushgit011/decentralized-clusters.git)
   cd decentralized-clusters
   ```

3. Build and start the containers:
   ```
   docker-compose up --build
   ```
   This will start the Ray head node, master node, and one worker node.

4. To scale the number of worker nodes, use:
   ```
   docker-compose up --scale worker=3
   ```
   This will start 3 worker containers.

## Usage

1. The Ray dashboard will be accessible at `http://localhost:8265`.

2. The master node API will be accessible at `http://localhost:8000`.

3. Use the `client.py` script to submit jobs and check their status:
   ```
   python client.py --action train
   python client.py --action inference
   python client.py --action status --job_id <job_id>
   ```

## Development

If you make changes to the Python files, you'll need to rebuild the Docker images:

```
docker-compose build
docker-compose up
```

## Requirements

See `requirements.txt` for the list of Python dependencies.

## Notes

- Ensure that your `master-node.py` and `worker-node.py` files are set to use the correct Ray address: `ray.init(address="ray://ray-head:10001")`.
- The `client.py` script should be set to connect to `http://localhost:8000` for the master node.

## Troubleshooting

If you encounter any issues:
1. Check the Docker logs: `docker-compose logs`
2. Ensure all required ports are open and not in use by other applications.
3. Verify that the Ray cluster is properly initialized in both master and worker nodes.

For more detailed information about each component, refer to the comments in the respective Python files.

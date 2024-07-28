import ray
import numpy as np

# Replace this with your actual head node IP
HEAD_NODE_IP = "your_head_node_ip_here"

@ray.remote
def train_model(data: list, epochs: int = 10) -> list:
    # This is a dummy training function. Replace with your actual training logic.
    model = np.array(data)
    for _ in range(epochs):
        model += np.random.rand(len(model)) * 0.1
    return model.tolist()

@ray.remote
def run_inference(model: list, input_data: list) -> float:
    # This is a dummy inference function. Replace with your actual inference logic.
    return np.dot(np.array(model), np.array(input_data))

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
        # Disconnect from the Ray cluster
        ray.shutdown()

    print("Training and inference completed successfully")

if __name__ == "__main__":
    main()

# File ends here
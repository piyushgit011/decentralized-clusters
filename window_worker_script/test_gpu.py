# test_gpu.py
import torch
import subprocess
import sys

def test_gpu():
    print("=== GPU Test Results ===")
    
    # Test nvidia-smi
    try:
        nvidia_smi = subprocess.run(['nvidia-smi'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print("\nnvidia-smi output:")
        print(nvidia_smi.stdout.decode())
    except Exception as e:
        print(f"nvidia-smi error: {e}")

    # Test PyTorch GPU support
    print("\nPyTorch GPU Support:")
    print(f"PyTorch version: {torch.__version__}")
    print(f"CUDA available: {torch.cuda.is_available()}")
    
    if torch.cuda.is_available():
        print(f"CUDA version: {torch.version.cuda}")
        print(f"Number of GPUs: {torch.cuda.device_count()}")
        for i in range(torch.cuda.device_count()):
            print(f"GPU {i}: {torch.cuda.get_device_name(i)}")
            # Try to allocate a small tensor on the GPU
            try:
                x = torch.tensor([1.0, 2.0], device=f'cuda:{i}')
                print(f"Successfully allocated tensor on GPU {i}")
            except Exception as e:
                print(f"Error allocating tensor on GPU {i}: {e}")
    else:
        print("No CUDA GPUs available")

if __name__ == "__main__":
    test_gpu()
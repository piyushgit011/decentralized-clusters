import os
import requests

def authenticate():
    provider_id = os.environ['PROVIDER_ID']
    auth_token = os.environ['AUTH_TOKEN']
    
    headers = {"Authorization": f"Bearer {auth_token}"}
    response = requests.get('http://67.205.167.215:8000/providers/me', headers=headers)
    
    if response.status_code == 200:
        print("Authentication successful")
        return True
    else:
        print("Authentication failed")
        return False

if __name__ == "__main__":
    if not authenticate():
        exit(1)
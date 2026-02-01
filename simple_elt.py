import requests
import json
from datetime import datetime

def extract():
    response = requests.get('https://jsonplaceholder.typicode.com/users')
    return response.json()

def transfrom(data):
    transfromed = []
    for user in data:
        transfromed.append({
            'user_id': user['id'],
            'name' : user['name'],
            'email': user['email'],
            'processed_at': datetime.now().isoformat()
        })
    return transfromed

def load(data):
    print(f"Loaded {len(data)} records:")
    for record in data[:3]:
        print(json.dumps(record, indent=2))

if __name__ == "__main__":
    print("Starting ELT...")
    raw_data = extract()
    clean_data = transfrom(raw_data)
    load(clean_data)
    print("ETL Complete!")
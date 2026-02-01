import requests
import json
from datetime import datetime

def extract():
    response = requests.get('https://jsonplaceholder.typicode.com/users')
    return response.json()

def transform(data):
    transformed = []
    for user in data:
        transformed.append({
            'user_id': user['id'],
            'name': user['name'],
            'email': user['email'],
            'processed_at': datetime.now().isoformat()
        })
    return transformed

def load(data):
    import psycopg2
    
    conn = psycopg2.connect(
        host="localhost",
        database="testdb",
        user="postgres",
        password="postgres"
    )
    cur = conn.cursor()
    
    for record in data:
        cur.execute("""
            INSERT INTO users (user_id, name, email, processed_at)
            VALUES (%s, %s, %s, %s)
        """, (record['user_id'], record['name'], 
              record['email'], record['processed_at']))
    
    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Loaded {len(data)} records into database")

if __name__ == "__main__":
    print("Starting ETL...")
    raw_data = extract()
    clean_data = transform(raw_data)
    load(clean_data)
    print("\n✅ ETL Complete!")
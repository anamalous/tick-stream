import os
import redis
import requests
import time
import json
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()
r = redis.Redis(host='localhost', port=6379, db=0)

API_KEY = os.getenv("ALPHA_API_KEY")
BOOTSTRAP_SERVERS = ['localhost:9092']

producer_config = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(producer_config)


def fetch_macro_data(function_name):
    url = f'https://www.alphavantage.co/query?function={function_name}&apikey={API_KEY}'
    r = requests.get(url)
    data = r.json()
    # Alpha Vantage returns the latest value as the first item in the 'data' list
    if "data" in data:
        latest = data["data"][0]
        return {
            "indicator": function_name,
            "value": float(latest["value"]),
            "date": latest["date"]
        }
    return None

# indicators to track
INDICATORS = ["FEDERAL_FUNDS_RATE", "CPI", "INFLATION"]

print("🌍 Macro Producer started. Fetching global context...")

while True:
    for indicator in INDICATORS:
        res = fetch_macro_data(indicator)
        if res:
            result = json.dumps(res).encode('utf-8')
            producer.produce(topic='indicators-live', value=result)

            redis_key = f"indicator:{indicator}"
            r.set(redis_key, json.dumps(res))

            print(f"📡 Broadcasted {indicator}: {res['value']} ({res['date']})")
            producer.flush()
    
    print("💤 Sleeping for 15 minutes...")
    time.sleep(900)
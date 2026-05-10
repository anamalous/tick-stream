import os
import time
import random
import requests
from confluent_kafka import Producer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import MessageField, SerializationContext
from dotenv import load_dotenv

load_dotenv()
NEWS_API_KEY = os.getenv("NEWS_KEY")
TICKERS = ["BTC", "ETH"] 
SEARCH_MAP = {"BTC": "BTCUSDT", "ETH": "ETHUSDT"}

seen_headlines = set()

sr_config = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(sr_config)

subject_name = 'market-sentiment-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

producer_config = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(producer_config)

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")

def fetch_real_news(ticker_query):
    """Fetches real headlines from NewsAPI for a specific ticker."""
    url = (f"https://newsapi.org/v2/everything?"
           f"q={ticker_query}&"
           f"sortBy=publishedAt&"
           f"language=en&"
           f"pageSize=5&"
           f"apiKey={NEWS_API_KEY}")
    
    try:
        response = requests.get(url)
        data = response.json()
        if data["status"] == "ok" and data["totalResults"] > 0:
            return data["articles"]
    except Exception as e:
        print(f"📡 API Fetch Error: {e}")
    return []

print("🎙️ News Desk is LIVE! Transitioned to Real-Time NewsAPI Stream...")

try:
    while True:
        for ticker in TICKERS:
            articles = fetch_real_news(ticker)
            
            for article in articles:
                headline = article['title']
                
                # Deduplication check
                if headline in seen_headlines:
                    continue
                
                seen_headlines.add(headline)
                if len(seen_headlines) > 500: 
                    seen_headlines.pop()

                now_micros = int(time.time() * 1000000)
                
                data = {
                    "ticker": SEARCH_MAP[ticker],
                    "source": article['source']['name'][:15], # schema compatibility
                    "headline": headline, 
                    "sentiment_score": 0,
                    "created_at": now_micros
                }
                
                ctx = SerializationContext('market-news-raw', MessageField.VALUE)
                        
                producer.produce(
                    topic='market-news-raw',
                    value=avro_serializer(data, ctx),
                    on_delivery=delivery_report
                )
                print(f"📰 Sent Real Intel: [{data['ticker']}] - {headline[:50]}...")

        producer.flush()
        
        print("⏳ Sleeping for 5 minutes to respect API limits...")
        time.sleep(300) 

except KeyboardInterrupt:
    print("Stopping News Desk...")
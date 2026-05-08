from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import MessageField, SerializationContext
from confluent_kafka import Producer, Consumer
import redis
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import json

sr_config = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(sr_config)

subject_name = 'market-sentiment-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str
avro_serializer = AvroSerializer(schema_registry_client, schema_str)
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

# load model
print("⏳ Loading FinBERT model... (this might take a minute)")
model_name = "ProsusAI/finbert"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForSequenceClassification.from_pretrained(model_name)

conf = {'bootstrap.servers': 'localhost:9092', 
    'group.id': 'sentiment-analysis-grou', 
    'auto.offset.reset': 'earliest'}
consumer = Consumer(conf)
consumer.subscribe(['market-news-raw'])

producer_config = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(producer_config)

def get_sentiment(text):
    inputs = tokenizer(text, return_tensors="pt", padding=True, truncation=True, max_length=512)
    outputs = model(**inputs)
    
    probabilities = torch.nn.functional.softmax(outputs.logits, dim=-1)
    
    pos = probabilities[0][0].item()
    neg = probabilities[0][1].item()
    
    # weighted score: positive - negative
    sentiment_score = pos - neg
    return round(sentiment_score, 4)

print("🚀 Sentiment Engine is LIVE. Listening for news...")

try:
    while True:
        message = consumer.poll(timeout=1.0)

        if message is None:
            continue
            
        if message.error():
            print(f"Consumer error: {message.error()}")
            continue

        try:
            ctx = SerializationContext('market-news-raw', MessageField.VALUE)
            news_data = avro_deserializer(message.value(), ctx)
        except Exception as e:
            print(f"❌ Avro Deserialization Failed: {e}")
            continue

        headline = news_data.get("headline", "")
    
        score = get_sentiment(headline)
        ctx = SerializationContext('market-sentiment-raw', MessageField.VALUE)

        enriched_news = {
            "ticker": news_data["ticker"],
            "source": news_data["source"],
            "headline": headline,
            "sentiment_score": score,
            "created_at": news_data["created_at"]
        }
    
        producer.produce(
            topic='market-sentiment-raw',
            value=avro_serializer(enriched_news, ctx)
        )
        producer.flush()

        enriched_news["created_at"]=enriched_news["created_at"].isoformat()
        r = redis.Redis(host='localhost', port=6379, db=0)
        r.lpush("recent_news", json.dumps(enriched_news))
        r.ltrim("recent_news", 0, 9)

        print(f"✅ Scored: {headline[:30]}... | Score: {score}")
finally:
    consumer.close()
    
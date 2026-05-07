import time
import random
from confluent_kafka import Producer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import MessageField, SerializationContext

sr_config = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(sr_config)

subject_name = 'market-sentiment-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

producer_config = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(producer_config)

headlines = [
    "Bitcoin breaks major resistance level!",
    "New regulation concerns in the crypto space.",
    "Major exchange adds support for new assets.",
    "Institutional inflows reach all-time high.",
    "Network upgrade successfully completed."
]

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")

print("🎙️ News Desk is live! Sending sentiment pulses...")

try:
    while True:
        now_micros = int(time.time() * 1000000)
        
        data = {
            "ticker": random.choice(["BTCUSDT", "ETHUSDT"]),
            "source": random.choice(["Twitter", "Reuters", "Bloomberg"]),
            "headline": random.choice(headlines), 
            "sentiment_score": float(random.uniform(-1.0, 1.0)),
            "created_at": now_micros
        }
        ctx = SerializationContext('market-sentiment-raw', MessageField.VALUE)
                
        producer.produce(
            topic='market-sentiment-raw',
            value=avro_serializer(data, ctx),
            on_delivery=delivery_report
        )

        producer.flush()
        
        print(f"📰 Sent: Score: {data['sentiment_score']}")

        # news simulated as less frequent than trades
        time.sleep(random.randint(5, 15)) 
except KeyboardInterrupt:
    print("Stopping News Desk...")
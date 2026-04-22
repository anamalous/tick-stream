import time
import random
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka import Producer

# 1. Setup Schema Registry Client
sr_config = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(sr_config)

# 2. Fetch latest schema from the registry - sync maintained
subject_name = 'market-trades-raw-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# 3. Define Serializer
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# 4. Setup Kafka Producer
producer_config = {
    'bootstrap.servers': 'localhost:9092',
}
producer = Producer(producer_config)

TICKERS = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'AAPL', 'TSLA']
EXCHANGES = ['BINANCE', 'NASDAQ', 'COINBASE']

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

print("🚀 Starting Market Alpha Producer... Press Ctrl+C to stop.")

try:
    while True:
        trade_data = {
            "ticker": random.choice(TICKERS), # matches against serialiser
            "price": round(random.uniform(50.0, 60000.0), 2),
            "size": random.randint(1, 100),
            "exchange": random.choice(EXCHANGES),
            "timestamp": int(time.time() * 1000000) 
        }

        ctx = SerializationContext('market-trades-raw', MessageField.VALUE)

        producer.produce(
            topic='market-trades-raw',
            value=avro_serializer(trade_data, ctx), 
            on_delivery=delivery_report
        )
        
        producer.poll(0)
        time.sleep(1)

except KeyboardInterrupt:
    print("\nStopping producer...")
finally:
    producer.flush()
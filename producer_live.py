import asyncio
import websockets
import json
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka import Producer

sr_config = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(sr_config)

subject_name = 'market-trades-raw-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

producer_config = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(producer_config)

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")

async def binance_stream():
    url = "wss://stream.binance.com:9443/ws/btcusdt@trade/ethusdt@trade"
    
    async with websockets.connect(url) as ws:
        print("✅ Connected to Binance WebSocket. Streaming Live Data...")
        
        while True:
            try:
                # 1. Receive raw JSON from Binance
                msg = await ws.recv()
                data = json.loads(msg)
                
                # 2. Map Binance response to YOUR Avro Schema
                # Binance keys: 's'=ticker, 'p'=price, 'q'=quantity, 'E'=EventTime
                trade_data = {
                    "ticker": data['s'],
                    "price": float(data['p']),
                    "size": int(float(data['q'])),
                    "exchange": "BINANCE",
                    "timestamp": int(data['E'] * 1000)
                }

                # 3. Serialize and Produce to Kafka
                ctx = SerializationContext('market-trades-raw', MessageField.VALUE)
                
                producer.produce(
                    topic='market-trades-raw',
                    value=avro_serializer(trade_data, ctx),
                    on_delivery=delivery_report
                )
                
                # Poll helps handle delivery callbacks
                producer.poll(0)

            except Exception as e:
                print(f"Error in stream: {e}")
                break

if __name__ == "__main__":
    try:
        asyncio.run(binance_stream())
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()
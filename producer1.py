# producer.py
import csv
import time
from kafka import KafkaProducer
import json

BROKER = "172.24.63.248:9092"

# Kafka producer with batching and optional compression
producer = KafkaProducer(
    bootstrap_servers=[BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    batch_size=256*1024,     # 256 KB batches
    linger_ms=50,            # wait max 50ms for more messages to batch
    compression_type='gzip', # optional: compress messages
    acks=1                   # faster ack (leader only)
)

DATASET = "dataset.csv"  # your assigned dataset

with open(DATASET) as f:
    reader = csv.DictReader(f)
    for row in reader:
        ts = row['ts']
        server_id = row['server_id']

        # send CPU
        producer.send('topic-cpu', {
            'ts': ts,
            'server_id': server_id,
            'cpu_pct': float(row['cpu_pct'])
        })

        # send Memory
        producer.send('topic-mem', {
            'ts': ts,
            'server_id': server_id,
            'mem_pct': float(row['mem_pct'])
        })

        # send Network
        producer.send('topic-net', {
            'ts': ts,
            'server_id': server_id,
            'net_in': float(row['net_in']),
            'net_out': float(row['net_out'])
        })

        # send Disk
        producer.send('topic-disk', {
            'ts': ts,
            'server_id': server_id,
            'disk_io': float(row['disk_io'])
        })

        # optional: very short sleep to avoid overwhelming broker
        time.sleep(0.001)  # 1ms

# flush remaining messages at the end
producer.flush()


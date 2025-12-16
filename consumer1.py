# consumer1.py
from kafka import KafkaConsumer
import json
import csv
import os
import time

BROKER = "172.24.63.248:9092"

# ⚙️ Subscribe to all topics from producer
consumer = KafkaConsumer(
    'topic-cpu', 'topic-mem', 'topic-net', 'topic-disk',
    bootstrap_servers=[BROKER],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=False,  # we'll commit manually after batches
    group_id='consumer1-group'
)

# ⚙️ Prepare CSV files for each topic
FILES = {
    'topic-cpu': 'consumer1_cpu_data.csv',
    'topic-mem': 'consumer1_mem_data.csv',
    'topic-net': 'consumer1_net_data.csv',
    'topic-disk': 'consumer1_disk_data.csv'
}

HEADERS = {
    'topic-cpu': ['ts', 'server_id', 'cpu_pct'],
    'topic-mem': ['ts', 'server_id', 'mem_pct'],
    'topic-net': ['ts', 'server_id', 'net_in', 'net_out'],
    'topic-disk': ['ts', 'server_id', 'disk_io']
}

writers = {}
files = {}

# Initialize CSV writers and add headers if files are empty
for topic, path in FILES.items():
    f = open(path, 'a', newline='')
    writer = csv.writer(f)
    files[topic] = f
    writers[topic] = writer
    if os.path.getsize(path) == 0:
        writer.writerow(HEADERS[topic])

print("✅ Consumer1 started — listening to 2 topics - cpu and mem.")
print("   Writing to consumer1_cpu_data.csv and consumer1_mem_data.csv files")

# ⚙️ Buffers for batched writes
buffers = {t: [] for t in FILES}
BATCH_SIZE = 500
FLUSH_INTERVAL = 5  # seconds
last_flush = time.time()

try:
    while True:
        # Poll all subscribed topics together
        records = consumer.poll(timeout_ms=500, max_records=1000)

        for tp, msgs in records.items():
            topic = tp.topic
            for record in msgs:
                v = record.value
                # Append to appropriate buffer
                if topic == 'topic-cpu':
                    buffers[topic].append([v['ts'], v['server_id'], v['cpu_pct']])
                elif topic == 'topic-mem':
                    buffers[topic].append([v['ts'], v['server_id'], v['mem_pct']])
                elif topic == 'topic-net':
                    buffers[topic].append([v['ts'], v['server_id'], v['net_in'], v['net_out']])
                elif topic == 'topic-disk':
                    buffers[topic].append([v['ts'], v['server_id'], v['disk_io']])

        # Check if it's time to flush
        if any(len(buf) >= BATCH_SIZE for buf in buffers.values()) or (time.time() - last_flush >= FLUSH_INTERVAL):
            for topic, buf in buffers.items():
                if buf:
                    writers[topic].writerows(buf)
                    buf.clear()
                    files[topic].flush()
            consumer.commit_async()  # mark offsets after each batch
            last_flush = time.time()

except KeyboardInterrupt:
    print("\n🛑 Stopping consumer1...")

finally:
    # Final flush before exit
    for topic, buf in buffers.items():
        if buf:
            writers[topic].writerows(buf)
            files[topic].flush()

    consumer.commit_sync()
    consumer.close()
    for f in files.values():
        f.close()

    print("✅ Consumer1 closed cleanly.")

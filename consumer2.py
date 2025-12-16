# consumer2.py
from kafka import KafkaConsumer
import json, csv, os

# 🔹 Broker IP
BROKER = "172.24.63.248:9092"

# 🔹 Create consumers for NET and DISK topics
consumer_net = KafkaConsumer(
    'topic-net',
    bootstrap_servers=[BROKER],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='consumer2-net-group'
)

consumer_disk = KafkaConsumer(
    'topic-disk',
    bootstrap_servers=[BROKER],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='consumer2-disk-group'
)

# 🔹 Prepare CSV files
net_file = open('consumer2_net_data.csv', 'a', newline='')
disk_file = open('consumer2_disk_data.csv', 'a', newline='')
net_writer = csv.writer(net_file)
disk_writer = csv.writer(disk_file)

# Write headers if empty
if os.path.getsize('consumer2_net_data.csv') == 0:
    net_writer.writerow(['ts', 'server_id', 'net_in', 'net_out'])

if os.path.getsize('consumer2_disk_data.csv') == 0:
    disk_writer.writerow(['ts', 'server_id', 'disk_io'])

print("✅ Consumer2 started — listening to topic-net and topic-disk")
print("   Writing to consumer2_net_data.csv and consumer2_disk_data.csv")

try:
    while True:
        # Poll NET topic
        net_msgs = consumer_net.poll(timeout_ms=1000, max_records=10)
        for tp, messages in net_msgs.items():
            for record in messages:
                v = record.value
                net_writer.writerow([v['ts'], v['server_id'], v.get('net_in', 0), v.get('net_out', 0)])
                net_file.flush()

        # Poll DISK topic
        disk_msgs = consumer_disk.poll(timeout_ms=1000, max_records=10)
        for tp, messages in disk_msgs.items():
            for record in messages:
                v = record.value
                disk_writer.writerow([v['ts'], v['server_id'], v.get('disk_io', 0)])
                disk_file.flush()

except KeyboardInterrupt:
    print("\n🛑 Stopping consumer2...")

finally:
    consumer_net.close()
    consumer_disk.close()
    net_file.close()
    disk_file.close()
    print("✅ Consumer2 closed cleanly.")

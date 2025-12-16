# Real-Time Server Monitoring and Anomaly Detection System
**Team 126 - Big Data Assignment 2**

## 📋 Project Overview

This project implements a real-time distributed data streaming and processing pipeline for server monitoring and anomaly detection. The system uses **Apache Kafka** for distributed message streaming and **Apache Spark** for real-time data analytics to identify performance anomalies across multiple server metrics.

### Key Features
- **Distributed Streaming**: Multi-topic Kafka architecture for different server metrics
- **Real-Time Processing**: Two independent consumer groups for parallel data ingestion
- **Anomaly Detection**: Dual Spark jobs for CPU/Memory and Network/Disk analysis
- **Sliding Window Analytics**: 30-second windows with 10-second slide intervals
- **Alert Generation**: Context-aware alerts for different anomaly patterns

---

## 🏗️ System Architecture

```
Dataset (CSV)
     ↓
Producer (producer1.py)
     ↓
Apache Kafka Broker (172.24.63.248:9092)
     ├── topic-cpu
     ├── topic-mem
     ├── topic-net
     └── topic-disk
     ↓
Consumer Layer
├── Consumer 1 (consumer1.py)
│   ├── Reads: topic-cpu, topic-mem
│   └── Outputs: consumer1_cpu_data.csv, consumer1_mem_data.csv
│
└── Consumer 2 (consumer2.py)
    ├── Reads: topic-net, topic-disk
    └── Outputs: consumer2_net_data.csv, consumer2_disk_data.csv
     ↓
Apache Spark Processing Layer
├── Spark Job 1 (spark_job1.py)
│   ├── Processes: CPU & Memory data
│   ├── Aggregation: Average over 30s windows
│   └── Output: team_126_CPU_MEM.csv
│
└── Spark Job 2 (spark_job2.py)
    ├── Processes: Network & Disk data
    ├── Aggregation: Maximum over 30s windows
    └── Output: team_126_NET_DISK.csv
```

---

## 📁 Project Structure

```
Team_126/
├── README.md                      # Project documentation (this file)
├── broker_commands.txt            # Kafka broker setup commands
│
├── dataset.csv                    # Input dataset (28,802 records)
│
├── producer1.py                   # Kafka producer for data streaming
│
├── consumer1.py                   # Consumer for CPU & Memory topics
├── consumer2.py                   # Consumer for Network & Disk topics
│
├── spark_job1.py                  # Spark job for CPU/MEM anomaly detection
├── spark_job2.py                  # Spark job for NET/DISK anomaly detection
│
├── consumer1_cpu_data.csv         # Consumer 1 output - CPU data
├── consumer1_mem_data.csv         # Consumer 1 output - Memory data
├── consumer2_net_data.csv         # Consumer 2 output - Network data
├── consumer2_disk_data.csv        # Consumer 2 output - Disk data
│
├── team_126_CPU_MEM.csv           # Final Spark Job 1 output (14,400 rows)
└── team_126_NET_DISK.csv          # Final Spark Job 2 output (14,400 rows)
```

---

## 🔧 Technology Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| **Message Broker** | Apache Kafka | 2.x+ |
| **Distributed Processing** | Apache Spark | 3.x+ |
| **Programming Language** | Python | 3.7+ |
| **Stream Processing** | kafka-python | Latest |
| **Data Processing** | PySpark | 3.x+ |

### Python Dependencies
```
kafka-python
pyspark
pandas
csv (built-in)
json (built-in)
```

---

## 🚀 Setup and Installation

### 1. Kafka Broker Setup

The Kafka broker is configured on a remote server at `172.24.63.248:9092`.

#### Start Zookeeper
```bash
cd /opt/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties
```

#### Configure Kafka Broker
Edit `config/server.properties`:
```properties
listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://172.24.63.248:9092
```

#### Start Kafka Server
```bash
cd /opt/kafka
bin/kafka-server-start.sh config/server.properties
```

#### Create Topics
```bash
bin/kafka-topics.sh --create --topic topic-cpu  --bootstrap-server 172.24.63.248:9092 --partitions 1 --replication-factor 1
bin/kafka-topics.sh --create --topic topic-mem  --bootstrap-server 172.24.63.248:9092 --partitions 1 --replication-factor 1
bin/kafka-topics.sh --create --topic topic-net  --bootstrap-server 172.24.63.248:9092 --partitions 1 --replication-factor 1
bin/kafka-topics.sh --create --topic topic-disk --bootstrap-server 172.24.63.248:9092 --partitions 1 --replication-factor 1
```

#### Verify Topics (Optional)
```bash
# Test producer
bin/kafka-console-producer.sh --topic topic-cpu --bootstrap-server 172.24.63.248:9092

# Test consumer
bin/kafka-console-consumer.sh --topic topic-cpu --bootstrap-server 172.24.63.248:9092 --from-beginning
```

### 2. Python Environment Setup

```bash
# Install required packages
pip install kafka-python pyspark pandas
```

---

## 🎯 Execution Workflow

### Step 1: Start the Kafka Producer
The producer reads from `dataset.csv` and publishes data to four Kafka topics.

```bash
python producer1.py
```

**What it does:**
- Reads 28,802 records from `dataset.csv`
- Splits data into 4 topics: `topic-cpu`, `topic-mem`, `topic-net`, `topic-disk`
- Uses batching (256KB) and GZIP compression for efficiency
- Streams data with 1ms delay between records

### Step 2: Start Consumer 1 (CPU & Memory)
Open a new terminal:
```bash
python consumer1.py
```

**What it does:**
- Subscribes to `topic-cpu` and `topic-mem`
- Uses consumer group `consumer1-group`
- Implements batch writing (500 records or 5-second flush interval)
- Outputs to `consumer1_cpu_data.csv` and `consumer1_mem_data.csv`
- Manual offset commit for reliability

### Step 3: Start Consumer 2 (Network & Disk)
Open another terminal:
```bash
python consumer2.py
```

**What it does:**
- Subscribes to `topic-net` and `topic-disk`
- Uses separate consumer groups: `consumer2-net-group` and `consumer2-disk-group`
- Real-time writing with auto-commit enabled
- Outputs to `consumer2_net_data.csv` and `consumer2_disk_data.csv`

### Step 4: Run Spark Job 1 (CPU & Memory Analysis)
After consumers have collected sufficient data:
```bash
spark-submit spark_job1.py
```

**What it does:**
- Loads `consumer1_cpu_data.csv` and `consumer1_mem_data.csv`
- Performs inner join on timestamp and server_id
- Calculates **average** CPU and Memory usage over 30-second sliding windows
- Applies anomaly thresholds:
  - **CPU Threshold**: 83.45%
  - **Memory Threshold**: 78.92%
- Generates alerts:
  - `"High CPU + Memory stress"` - Both exceed thresholds
  - `"CPU spike suspected"` - Only CPU exceeds threshold
  - `"Memory saturation suspected"` - Only Memory exceeds threshold
  - Empty string - Normal operation
- Outputs 14,400 rows to `team_126_CPU_MEM.csv`

### Step 5: Run Spark Job 2 (Network & Disk Analysis)
```bash
spark-submit spark_job2.py
```

**What it does:**
- Loads `consumer2_net_data.csv` and `consumer2_disk_data.csv`
- Performs inner join on timestamp and server_id
- Calculates **maximum** Network In and Disk I/O over 30-second sliding windows
- Applies anomaly thresholds:
  - **Network In Threshold**: 4149.79 KB/s
  - **Disk I/O Threshold**: 1925.47 IOPS
- Generates alerts:
  - `"Network flood + Disk thrash suspected"` - Both exceed thresholds
  - `"Possible DDoS"` - Only Network exceeds threshold
  - `"Disk thrash suspected"` - Only Disk exceeds threshold
  - Empty string - Normal operation
- Outputs 14,400 rows to `team_126_NET_DISK.csv`

---

## 📊 Data Flow and Formats

### Input Dataset Schema
```csv
ts,server_id,cpu_pct,mem_pct,net_in,net_out,disk_io
20:53:00,server_1,34.74,46.41,359.45,489.36,33.5
```
- **Total Records**: 28,802
- **Servers**: 20 (server_1 to server_20)
- **Time Range**: 20:53:00 onwards (1-second granularity)

### Kafka Message Format (JSON)
```json
// topic-cpu
{"ts": "20:53:00", "server_id": "server_1", "cpu_pct": 34.74}

// topic-mem
{"ts": "20:53:00", "server_id": "server_1", "mem_pct": 46.41}

// topic-net
{"ts": "20:53:00", "server_id": "server_1", "net_in": 359.45, "net_out": 489.36}

// topic-disk
{"ts": "20:53:00", "server_id": "server_1", "disk_io": 33.5}
```

### Final Output Schema

#### team_126_CPU_MEM.csv (14,400 rows)
```csv
server_id,window_start,window_end,avg_cpu,avg_mem,alert
server_1,20:53:20,20:53:50,45.32,52.18,
server_1,20:53:30,20:54:00,48.76,55.42,
server_2,21:05:40,21:06:10,85.12,82.34,"High CPU + Memory stress"
```

#### team_126_NET_DISK.csv (14,400 rows)
```csv
server_id,window_start,window_end,max_net_in,max_disk_io,alert
server_1,20:53:20,20:53:50,1200.45,850.32,
server_5,21:10:50,21:11:20,4500.89,2100.45,"Network flood + Disk thrash suspected"
```

---

## 🔍 Key Implementation Details

### Producer Optimizations
- **Batch Size**: 256 KB for network efficiency
- **Linger Time**: 50ms to accumulate messages
- **Compression**: GZIP to reduce bandwidth
- **ACK Level**: 1 (leader acknowledgment only for speed)
- **Throttling**: 1ms sleep between records to prevent broker overload

### Consumer 1 Optimizations
- **Batch Writing**: Accumulates 500 records before disk write
- **Flush Interval**: 5 seconds to ensure timely persistence
- **Manual Commit**: Commits offsets after successful batch write
- **Multi-topic Subscription**: Single consumer polls all topics efficiently

### Consumer 2 Design
- **Dual Consumer Strategy**: Separate consumers for Network and Disk
- **Independent Consumer Groups**: Allows parallel consumption
- **Auto-commit**: Enabled for simpler offset management
- **Real-time Writing**: Immediate flush after each record

### Spark Job 1 Features
- **Time Alignment**: Adjusts timestamps to 10-second boundaries
- **Window Aggregation**: 30-second duration, 10-second slide
- **Precision Rounding**: Custom "round-half-up" UDF for financial-grade precision
- **Row Filtering**: Trims output to rows 3-722 per server (720 rows × 20 servers = 14,400)

### Spark Job 2 Features
- **MAX Aggregation**: Uses maximum values instead of averages
- **Empty String Alerts**: Returns `""` for normal conditions (not null)
- **Standard Rounding**: Uses Spark's built-in round function (2 decimal places)
- **Identical Windowing**: Same time alignment logic as Job 1

---

## ⚠️ Anomaly Detection Logic

### CPU & Memory Anomalies (Spark Job 1)
| Condition | Alert Message |
|-----------|--------------|
| `avg_cpu > 83.45` AND `avg_mem > 78.92` | "High CPU + Memory stress" |
| `avg_cpu > 83.45` AND `avg_mem ≤ 78.92` | "CPU spike suspected" |
| `avg_cpu ≤ 83.45` AND `avg_mem > 78.92` | "Memory saturation suspected" |
| Both within thresholds | `""` (empty string) |

### Network & Disk Anomalies (Spark Job 2)
| Condition | Alert Message |
|-----------|--------------|
| `max_net_in > 4149.79` AND `max_disk_io > 1925.47` | "Network flood + Disk thrash suspected" |
| `max_net_in > 4149.79` AND `max_disk_io ≤ 1925.47` | "Possible DDoS" |
| `max_net_in ≤ 4149.79` AND `max_disk_io > 1925.47` | "Disk thrash suspected" |
| Both within thresholds | `""` (empty string) |

---

## 🧪 Testing and Validation

### Verify Kafka Topics
```bash
# List all topics
bin/kafka-topics.sh --list --bootstrap-server 172.24.63.248:9092

# Check topic details
bin/kafka-topics.sh --describe --topic topic-cpu --bootstrap-server 172.24.63.248:9092
```

### Monitor Consumer Lag
```bash
bin/kafka-consumer-groups.sh --bootstrap-server 172.24.63.248:9092 \
  --group consumer1-group --describe
```

### Validate Output Row Counts
```python
# Python validation
import pandas as pd

# Check CPU/MEM output
df1 = pd.read_csv('team_126_CPU_MEM.csv')
print(f"CPU/MEM rows: {len(df1)}")  # Should be 14,400

# Check NET/DISK output
df2 = pd.read_csv('team_126_NET_DISK.csv')
print(f"NET/DISK rows: {len(df2)}")  # Should be 14,400

# Verify 20 servers, 720 rows each
print(df1.groupby('server_id').size())
```

---

## 🐛 Troubleshooting

### Issue: Producer cannot connect to Kafka
**Solution**: Verify broker IP and port. Check firewall rules and ensure Kafka is running.
```bash
# Test connectivity
telnet 172.24.63.248 9092
```

### Issue: Consumers not receiving messages
**Solution**: 
1. Check if producer has finished streaming
2. Verify consumer group IDs are unique
3. Use `auto_offset_reset='earliest'` to read from beginning

### Issue: Spark job fails with timestamp parsing errors
**Solution**: Ensure CSV headers are present. Check timestamp format consistency.
```python
# Add this to debug
df.printSchema()
df.show(5)
```

### Issue: Output row count mismatch
**Solution**: Check the row filtering logic (rows 3-722). Ensure all 20 servers have sufficient data.

---

## 📈 Performance Metrics

| Metric | Value |
|--------|-------|
| **Total Input Records** | 28,802 |
| **Kafka Topics** | 4 |
| **Servers Monitored** | 20 |
| **Window Duration** | 30 seconds |
| **Slide Interval** | 10 seconds |
| **Final Output Records** | 14,400 per job |
| **Producer Throughput** | ~1000 msgs/sec |
| **Consumer Latency** | <100ms |
| **Spark Processing Time** | ~30-60 seconds per job |

---

## 👥 Team Information

**Team Number**: 126  
**Course**: Big Data (SEM 5)  
**Assignment**: Assignment 2 - Real-Time Stream Processing

---

## 📝 License

This project is part of an academic assignment. All rights reserved by Team 126.

---

## 🔗 References

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [kafka-python Library](https://kafka-python.readthedocs.io/)
- [PySpark API Documentation](https://spark.apache.org/docs/latest/api/python/)

---

## 📞 Support

For questions or issues related to this project, please contact the team members or refer to the course instructor.

---

**Last Updated**: December 17, 2025  
**Version**: 1.0

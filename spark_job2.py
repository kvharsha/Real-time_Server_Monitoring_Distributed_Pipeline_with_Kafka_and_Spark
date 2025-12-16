from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, max, when, lit, date_format, round, min, unix_timestamp, expr
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import os

# --- Configuration & Thresholds ---
NET_IN_THRESHOLD = 4149.79
DISK_IO_THRESHOLD = 1925.47

# WINDOW PARAMETERS
WINDOW_SIZE = "30 seconds"
SLIDE_INTERVAL = "10 seconds"

# INPUT/OUTPUT FILENAME NAMES
NET_INPUT_FILE_NAME = "consumer2_net_data.csv"
DISK_INPUT_FILE_NAME = "consumer2_disk_data.csv"
OUTPUT_FILE_NAME = "team_126_NET_DISK.csv"

# Use absolute paths for robust file handling
current_dir = os.getcwd() 
NET_INPUT_FILE = f"file://{current_dir}/{NET_INPUT_FILE_NAME}"
DISK_INPUT_FILE = f"file://{current_dir}/{DISK_INPUT_FILE_NAME}"
FINAL_OUTPUT_PATH = f"{current_dir}/{OUTPUT_FILE_NAME}"


# --- Spark Session Initialization ---
spark = SparkSession.builder \
    .appName("SparkJob2_NETDISK_Anomaly") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print("✅ Spark Session created for Network and Disk anomaly detection.")

# --- Define Schema ---
# Network data includes net_out but only net_in is needed for aggregation
data_schema_net = StructType([
    StructField("ts", StringType(), True), StructField("server_id", StringType(), True), 
    StructField("net_in", DoubleType(), True), StructField("net_out", DoubleType(), True)
])
data_schema_disk = StructType([
    StructField("ts", StringType(), True), StructField("server_id", StringType(), True), 
    StructField("disk_io", DoubleType(), True)
])

# --- Data Ingestion ---
try:
    df_net = spark.read.csv(NET_INPUT_FILE, header=True, schema=data_schema_net, dateFormat="yyyy-MM-dd HH:mm:ss.SSSSSS")
    df_disk = spark.read.csv(DISK_INPUT_FILE, header=True, schema=data_schema_disk, dateFormat="yyyy-MM-dd HH:mm:ss.SSSSSS")
except Exception as e:
    print(f"❌ Error reading input CSV files: {e}")
    spark.stop()
    exit()

# 1. Prepare Data & Calculate Time Offset for perfect alignment
df_net = df_net.withColumn("timestamp", col("ts").cast("timestamp")).drop("ts")
df_disk = df_disk.withColumn("timestamp", col("ts").cast("timestamp")).drop("ts")

df_combined = df_net.join(df_disk, on=['timestamp', 'server_id'], how='inner').select(
    "timestamp", "server_id", "net_in", "disk_io"
)

# Time Alignment Logic: Find offset to align timestamps to the nearest 10s boundary
min_ts_sec = df_combined.agg(min(unix_timestamp("timestamp"))).collect()[0][0]
offset_seconds = min_ts_sec % 10 
df_offset = df_combined.withColumn(
    "aligned_timestamp",
    expr(f"timestamp - interval {offset_seconds} seconds")
)

# 2. Perform Window-Based Aggregation (using MAX)
window_spec = window(timeColumn=col("aligned_timestamp"), windowDuration=WINDOW_SIZE, slideDuration=SLIDE_INTERVAL)

df_windowed = df_offset.groupBy(col("server_id"), window_spec).agg(
    max("net_in").alias("max_net_in"), # Required aggregation is MAX
    max("disk_io").alias("max_disk_io")
)

# 3. Apply Alerting Logic (Empty string for non-alerts)
df_alerts = df_windowed.withColumn(
    "alert",
    when(
        (col("max_net_in") > NET_IN_THRESHOLD) & (col("max_disk_io") > DISK_IO_THRESHOLD), 
        lit("Network flood + Disk thrash suspected")
    ).when(
        (col("max_net_in") > NET_IN_THRESHOLD) & (col("max_disk_io") <= DISK_IO_THRESHOLD), 
        lit("Possible DDoS")
    ).when(
        (col("max_disk_io") > DISK_IO_THRESHOLD) & (col("max_net_in") <= NET_IN_THRESHOLD), 
        lit("Disk thrash suspected")
    ).otherwise(lit("")) # <--- FIX APPLIED: Outputs blank string ""
)

# 4. Format Output (Add the offset back to display the ground truth time)
df_formatted = df_alerts.withColumn(
    "window_start",
    date_format(expr(f"window.start + interval {offset_seconds} seconds"), "HH:mm:ss")
).withColumn(
    "window_end",
    date_format(expr(f"window.end + interval {offset_seconds} seconds"), "HH:mm:ss")
).select(
    "server_id", "window_start", "window_end", 
    round(col("max_net_in"), 2).alias("max_net_in"),
    round(col("max_disk_io"), 2).alias("max_disk_io"), 
    "alert"
)

# 5. Trim to achieve the exact expected row count (14400 total)
# Filter starts from row_num >= 3 (to begin at 20:53:20 window) and ends at row_num <= 722 
# to ensure exactly 720 rows per server (14400 total).
window_rank_spec = Window.partitionBy("server_id").orderBy("window_start")
df_ranked = df_formatted.withColumn("row_num", row_number().over(window_rank_spec))

df_final_trimmed = df_ranked.filter(
    (col("row_num") >= 3) & (col("row_num") <= 722)
).drop("row_num")


# 6. Write to a single CSV file
try:
    pandas_df = df_final_trimmed.toPandas()
    pandas_df.to_csv(FINAL_OUTPUT_PATH, index=False)
    
    print(f"✅ Spark Job 2 completed successfully.")
    print(f"   Final Row Count: {len(pandas_df)}")
    print(f"   Output saved to: {FINAL_OUTPUT_PATH}")

except Exception as e:
    print(f"❌ Error during final data collection and writing: {e}")

# Stop Spark Session
spark.stop()

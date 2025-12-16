from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, avg, when, lit, date_format, min, unix_timestamp, expr
)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import os
from decimal import Decimal, ROUND_HALF_UP
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType as PySparkFloatType

# --- Configuration & Thresholds ---
CPU_THRESHOLD = 83.45
MEM_THRESHOLD = 78.92
WINDOW_SIZE = "30 seconds"
SLIDE_INTERVAL = "10 seconds"

# INPUT/OUTPUT FILE NAMES
CPU_INPUT_FILE_NAME = "consumer1_cpu_data.csv"
MEM_INPUT_FILE_NAME = "consumer1_mem_data.csv"
OUTPUT_FILE_NAME = "team_126_CPU_MEM.csv"

# Use absolute paths for robust file handling
current_dir = os.getcwd()
CPU_INPUT_FILE = f"file://{current_dir}/{CPU_INPUT_FILE_NAME}"
MEM_INPUT_FILE = f"file://{current_dir}/{MEM_INPUT_FILE_NAME}"
FINAL_OUTPUT_PATH = f"{current_dir}/{OUTPUT_FILE_NAME}"

# --- Spark Session Initialization ---
spark = SparkSession.builder \
    .appName("SparkJob1_CPUMEM_Anomaly") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print("✅ Spark Session created for CPU and Memory anomaly detection.")

# --- Define Schema (FLOAT type instead of DOUBLE) ---
data_schema_cpu = StructType([
    StructField("ts", StringType(), True),
    StructField("server_id", StringType(), True),
    StructField("cpu_pct", FloatType(), True)
])

data_schema_mem = StructType([
    StructField("ts", StringType(), True),
    StructField("server_id", StringType(), True),
    StructField("mem_pct", FloatType(), True)
])

# --- Data Ingestion ---
try:
    df_cpu = spark.read.csv(
        CPU_INPUT_FILE, header=True, schema=data_schema_cpu, dateFormat="yyyy-MM-dd HH:mm:ss.SSSSSS"
    )
    df_mem = spark.read.csv(
        MEM_INPUT_FILE, header=True, schema=data_schema_mem, dateFormat="yyyy-MM-dd HH:mm:ss.SSSSSS"
    )
except Exception as e:
    print(f"❌ Error reading input CSV files: {e}")
    spark.stop()
    exit()

# --- Prepare Data ---
df_cpu = df_cpu.withColumn("timestamp", col("ts").cast("timestamp")).drop("ts")
df_mem = df_mem.withColumn("timestamp", col("ts").cast("timestamp")).drop("ts")

df_combined = df_cpu.join(df_mem, on=["timestamp", "server_id"], how="inner").select(
    "timestamp", "server_id", "cpu_pct", "mem_pct"
)

# --- Time Alignment Logic ---
min_ts_sec = df_combined.agg(min(unix_timestamp("timestamp"))).collect()[0][0]
offset_seconds = min_ts_sec % 10
df_offset = df_combined.withColumn(
    "aligned_timestamp", expr(f"timestamp - interval {offset_seconds} seconds")
)

# --- Window-Based Aggregation ---
window_spec = window(
    timeColumn=col("aligned_timestamp"),
    windowDuration=WINDOW_SIZE,
    slideDuration=SLIDE_INTERVAL
)

df_windowed = df_offset.groupBy(col("server_id"), window_spec).agg(
    avg("cpu_pct").alias("avg_cpu"),
    avg("mem_pct").alias("avg_mem")
)

# --- Alerting Logic ---
df_alerts = df_windowed.withColumn(
    "alert",
    when((col("avg_cpu") > CPU_THRESHOLD) & (col("avg_mem") > MEM_THRESHOLD), lit("High CPU + Memory stress"))
    .when((col("avg_cpu") > CPU_THRESHOLD) & (col("avg_mem") <= MEM_THRESHOLD), lit("CPU spike suspected"))
    .when((col("avg_mem") > MEM_THRESHOLD) & (col("avg_cpu") <= CPU_THRESHOLD), lit("Memory saturation suspected"))
    .otherwise(lit(""))
)

# --- Define "Round Half Up" UDF ---
def round_half_up(value, ndigits=2):
    if value is None:
        return None
    return float(Decimal(value).quantize(Decimal(f"1.{'0'*ndigits}"), rounding=ROUND_HALF_UP))

udf_round_half_up = udf(round_half_up, PySparkFloatType())

# --- Format Output (Apply round-half-up rounding) ---
df_formatted = df_alerts.withColumn(
    "window_start",
    date_format(expr(f"window.start + interval {offset_seconds} seconds"), "HH:mm:ss")
).withColumn(
    "window_end",
    date_format(expr(f"window.end + interval {offset_seconds} seconds"), "HH:mm:ss")
).withColumn(
    "avg_cpu", udf_round_half_up(col("avg_cpu"))
).withColumn(
    "avg_mem", udf_round_half_up(col("avg_mem"))
).select(
    "server_id", "window_start", "window_end", "avg_cpu", "avg_mem", "alert"
)

# --- Trim to Exact Expected Row Count ---
window_rank_spec = Window.partitionBy("server_id").orderBy("window_start")
df_ranked = df_formatted.withColumn("row_num", row_number().over(window_rank_spec))

df_final_trimmed = df_ranked.filter(
    (col("row_num") >= 3) & (col("row_num") <= 722)
).drop("row_num")

# --- Write to a Single CSV File ---
try:
    pandas_df = df_final_trimmed.toPandas()
    pandas_df.to_csv(FINAL_OUTPUT_PATH, index=False)
    print(f"✅ Spark Job 1 completed successfully.")
    print(f"   Final Row Count: {len(pandas_df)}")
    print(f"   Output saved to: {FINAL_OUTPUT_PATH}")
except Exception as e:
    print(f"❌ Error during final data collection and writing: {e}")

# --- Stop Spark Session ---
spark.stop()

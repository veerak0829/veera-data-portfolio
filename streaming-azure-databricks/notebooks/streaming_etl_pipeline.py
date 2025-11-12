# Databricks notebook source
# MAGIC %md
# MAGIC # Real-Time Streaming ETL Pipeline - IoT Sensor Data
# MAGIC 
# MAGIC ## Overview
# MAGIC This notebook demonstrates real-time stream processing using Azure Event Hubs and Databricks Structured Streaming.
# MAGIC 
# MAGIC **Pipeline Components:**
# MAGIC 1. **Stream Ingestion**: Read events from Azure Event Hubs
# MAGIC 2. **Real-time Transformation**: Parse, validate, and enrich streaming data
# MAGIC 3. **Windowed Aggregations**: Calculate metrics over time windows
# MAGIC 4. **Anomaly Detection**: Identify threshold violations in real-time
# MAGIC 5. **Delta Lake Sink**: Write streaming data with exactly-once semantics
# MAGIC 
# MAGIC **Data Source:** IoT sensor events (temperature, humidity, pressure)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Environment Setup & Configuration

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, current_timestamp,
    avg, max as _max, min as _min, count, sum as _sum,
    when, lit, round as _round, unix_timestamp, expr,
    from_unixtime, date_format, hour, minute
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    TimestampType, IntegerType
)
import json
import random
from datetime import datetime, timedelta

print("✅ Imports completed successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration Parameters

# COMMAND ----------

# Azure Event Hubs Configuration
# In production, use Databricks secrets for credentials
EVENT_HUB_CONNECTION_STRING = "Endpoint=sb://YOUR_NAMESPACE.servicebus.windows.net/;..."
EVENT_HUB_NAME = "sensor-events"

# For demonstration, we'll simulate streaming data instead
USE_SIMULATED_STREAM = True

# Checkpoint and output paths
CHECKPOINT_PATH = "/tmp/streaming/checkpoints/"
OUTPUT_PATH = "/tmp/streaming/output/"
AGGREGATED_PATH = "/tmp/streaming/aggregated/"

# Streaming configuration
TRIGGER_INTERVAL = "10 seconds"
WATERMARK_DELAY = "10 seconds"

print("Configuration loaded successfully!")
print(f"Checkpoint Path: {CHECKPOINT_PATH}")
print(f"Output Path: {OUTPUT_PATH}")
print(f"Aggregated Path: {AGGREGATED_PATH}")
print(f"Trigger Interval: {TRIGGER_INTERVAL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Define Schema for Streaming Events

# COMMAND ----------

# Define schema for incoming IoT sensor events (JSON format)
sensor_event_schema = StructType([
    StructField("sensor_id", StringType(), False),
    StructField("device_type", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("value", DoubleType(), False),
    StructField("unit", StringType(), True),
    StructField("location", StringType(), True),
    StructField("status", StringType(), True)
])

print("Schema defined successfully!")
sensor_event_schema

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Simulate Streaming Data Source (For Portfolio Demonstration)

# COMMAND ----------

if USE_SIMULATED_STREAM:
    # Create a simulated streaming data source using rate source
    # This generates events continuously for demonstration
    
    # Generate sample IoT sensor data
    def generate_sensor_event(row_id):
        """Generate a single sensor event"""
        sensor_types = {
            'temperature': {'unit': 'fahrenheit', 'min': 32, 'max': 100, 'threshold_high': 90, 'threshold_low': 40},
            'humidity': {'unit': 'percent', 'min': 20, 'max': 95, 'threshold_high': 85, 'threshold_low': 30},
            'pressure': {'unit': 'psi', 'min': 14, 'max': 18, 'threshold_high': 17, 'threshold_low': 15}
        }
        
        locations = ['warehouse-1', 'warehouse-2', 'factory-floor', 'cold-storage', 'loading-dock']
        device_type = random.choice(list(sensor_types.keys()))
        config = sensor_types[device_type]
        
        # Generate value (occasionally create anomalies)
        if random.random() < 0.05:  # 5% anomaly rate
            value = random.uniform(config['max'], config['max'] * 1.2)
            status = 'warning'
        else:
            value = random.uniform(config['min'], config['max'])
            status = 'normal'
        
        event = {
            'sensor_id': f"SENSOR{random.randint(1, 20):03d}",
            'device_type': device_type,
            'timestamp': datetime.now().isoformat(),
            'value': round(value, 2),
            'unit': config['unit'],
            'location': random.choice(locations),
            'status': status
        }
        
        return json.dumps(event)
    
    # Create streaming DataFrame using rate source
    # This generates rows at a specified rate for testing
    streaming_df = spark.readStream \
        .format("rate") \
        .option("rowsPerSecond", 10) \
        .load() \
        .selectExpr("CAST(value AS STRING) as row_id") \
        .selectExpr("*", "CAST(CAST(row_id AS INT) AS STRING) as event_json_temp")
    
    # Register UDF to generate sensor events
    from pyspark.sql.functions import udf
    generate_event_udf = udf(generate_sensor_event, StringType())
    
    # Apply UDF to generate JSON events
    streaming_df = streaming_df \
        .withColumn("event_json", generate_event_udf(col("row_id"))) \
        .select("event_json")
    
    print("✅ Simulated streaming source created (10 events/second)")
    print("In production, this would be replaced with Event Hubs connection")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Parse Streaming JSON Events

# COMMAND ----------

# Parse JSON events and extract fields
parsed_stream = streaming_df \
    .select(from_json(col("event_json"), sensor_event_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_timestamp", to_timestamp(col("timestamp"))) \
    .withColumn("ingestion_time", current_timestamp()) \
    .drop("timestamp") \
    .withColumnRenamed("event_timestamp", "timestamp")

print("✅ JSON parsing configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Data Quality & Enrichment

# COMMAND ----------

# Apply data quality checks and enrichments
enriched_stream = parsed_stream \
    .filter(col("sensor_id").isNotNull()) \
    .filter(col("value") >= 0) \
    .withColumn("hour_of_day", hour(col("timestamp"))) \
    .withColumn("minute_of_hour", minute(col("timestamp"))) \
    .withColumn("processing_time", current_timestamp())

# Add anomaly detection flags based on device type
enriched_stream = enriched_stream \
    .withColumn("is_anomaly",
        when((col("device_type") == "temperature") & 
             ((col("value") > 90) | (col("value") < 40)), True)
        .when((col("device_type") == "humidity") & 
             ((col("value") > 85) | (col("value") < 30)), True)
        .when((col("device_type") == "pressure") & 
             ((col("value") > 17) | (col("value") < 15)), True)
        .otherwise(False)
    ) \
    .withColumn("severity",
        when(col("is_anomaly") == True, "high")
        .otherwise("normal")
    )

print("✅ Data quality checks and enrichment configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Write Raw Streaming Events to Delta Lake

# COMMAND ----------

# Write the enriched stream to Delta Lake with checkpointing
# This provides exactly-once processing semantics

raw_stream_query = enriched_stream \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/raw") \
    .trigger(processingTime=TRIGGER_INTERVAL) \
    .start(OUTPUT_PATH)

print("✅ Raw streaming write started")
print(f"Query ID: {raw_stream_query.id}")
print(f"Status: {raw_stream_query.status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Real-Time Windowed Aggregations

# COMMAND ----------

# Calculate aggregated metrics over tumbling windows
# Window size: 1 minute
# Watermark: 10 seconds (handle late data)

windowed_aggregations = enriched_stream \
    .withWatermark("timestamp", WATERMARK_DELAY) \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("sensor_id"),
        col("device_type"),
        col("location")
    ) \
    .agg(
        count("*").alias("event_count"),
        _round(avg("value"), 2).alias("avg_value"),
        _round(_min("value"), 2).alias("min_value"),
        _round(_max("value"), 2).alias("max_value"),
        _sum(when(col("is_anomaly") == True, 1).otherwise(0)).alias("anomaly_count")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("sensor_id"),
        col("device_type"),
        col("location"),
        col("event_count"),
        col("avg_value"),
        col("min_value"),
        col("max_value"),
        col("anomaly_count")
    ) \
    .withColumn("processing_timestamp", current_timestamp())

print("✅ Windowed aggregations configured (1-minute tumbling window)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Write Aggregated Results to Delta Lake

# COMMAND ----------

# Write aggregated results to separate Delta table
aggregation_query = windowed_aggregations \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/aggregated") \
    .trigger(processingTime=TRIGGER_INTERVAL) \
    .start(AGGREGATED_PATH)

print("✅ Aggregated streaming write started")
print(f"Query ID: {aggregation_query.id}")
print(f"Status: {aggregation_query.status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Monitor Streaming Queries

# COMMAND ----------

import time

# Let the stream run for a demo period
print("Streaming queries running... (will run for 30 seconds for demo)")
print("\nMonitoring streaming progress:\n")

for i in range(6):  # Monitor for 30 seconds (6 intervals of 5 seconds)
    time.sleep(5)
    
    print(f"--- Check {i+1} (after {(i+1)*5} seconds) ---")
    
    # Raw stream status
    if raw_stream_query.isActive:
        progress = raw_stream_query.lastProgress
        if progress:
            print(f"Raw Stream - Rows processed: {progress.get('numInputRows', 0)}, "
                  f"Batch: {progress.get('batchId', 'N/A')}")
    
    # Aggregation stream status
    if aggregation_query.isActive:
        progress = aggregation_query.lastProgress
        if progress:
            print(f"Aggregation Stream - Rows processed: {progress.get('numInputRows', 0)}, "
                  f"Batch: {progress.get('batchId', 'N/A')}")
    
    print()

print("✅ Monitoring complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Query Streaming Results

# COMMAND ----------

# Query the raw streaming events Delta table
print("=" * 70)
print("RAW STREAMING EVENTS - Sample Data")
print("=" * 70)

raw_events_df = spark.read.format("delta").load(OUTPUT_PATH)
print(f"\nTotal events processed: {raw_events_df.count()}")
print("\nSample events:")
raw_events_df.orderBy(col("timestamp").desc()).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Query Windowed Aggregations

# COMMAND ----------

print("=" * 70)
print("WINDOWED AGGREGATIONS - Sample Data")
print("=" * 70)

aggregated_df = spark.read.format("delta").load(AGGREGATED_PATH)
print(f"\nTotal aggregation windows: {aggregated_df.count()}")
print("\nSample aggregations:")
aggregated_df.orderBy(col("window_start").desc()).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Anomaly Detection Report

# COMMAND ----------

print("=" * 70)
print("ANOMALY DETECTION REPORT")
print("=" * 70)

# Count anomalies by device type
print("\n🚨 Anomalies by Device Type:")
raw_events_df.filter(col("is_anomaly") == True) \
    .groupBy("device_type") \
    .count() \
    .orderBy(col("count").desc()) \
    .show()

# Count anomalies by location
print("\n📍 Anomalies by Location:")
raw_events_df.filter(col("is_anomaly") == True) \
    .groupBy("location") \
    .count() \
    .orderBy(col("count").desc()) \
    .show()

# Show recent anomalies
print("\n🔴 Recent Anomalies (Last 10):")
raw_events_df.filter(col("is_anomaly") == True) \
    .select("timestamp", "sensor_id", "device_type", "value", "unit", "location") \
    .orderBy(col("timestamp").desc()) \
    .show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Performance Metrics

# COMMAND ----------

print("=" * 70)
print("STREAMING PERFORMANCE METRICS")
print("=" * 70)

# Raw stream metrics
if raw_stream_query.lastProgress:
    progress = raw_stream_query.lastProgress
    print("\n📊 Raw Stream Metrics:")
    print(f"   Input Rows: {progress.get('numInputRows', 0)}")
    print(f"   Processing Rate: {progress.get('processedRowsPerSecond', 0):.2f} rows/sec")
    print(f"   Batch Duration: {progress.get('batchDuration', 0)} ms")
    print(f"   Latest Batch ID: {progress.get('batchId', 'N/A')}")

# Aggregation stream metrics
if aggregation_query.lastProgress:
    progress = aggregation_query.lastProgress
    print("\n📊 Aggregation Stream Metrics:")
    print(f"   Input Rows: {progress.get('numInputRows', 0)}")
    print(f"   Processing Rate: {progress.get('processedRowsPerSecond', 0):.2f} rows/sec")
    print(f"   Batch Duration: {progress.get('batchDuration', 0)} ms")

print("\n" + "=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Stop Streaming Queries

# COMMAND ----------

# Stop the streaming queries (for demo purposes)
print("Stopping streaming queries...")

if raw_stream_query.isActive:
    raw_stream_query.stop()
    print("✅ Raw stream query stopped")

if aggregation_query.isActive:
    aggregation_query.stop()
    print("✅ Aggregation query stopped")

print("\nAll streaming queries stopped successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 16. Pipeline Summary

# COMMAND ----------

print("=" * 70)
print("STREAMING ETL PIPELINE EXECUTION SUMMARY")
print("=" * 70)
print("\n✅ Pipeline Status: COMPLETED SUCCESSFULLY")
print("\n📊 Streaming Outputs:")
print(f"   - Raw Events: {OUTPUT_PATH}")
print(f"   - Aggregations: {AGGREGATED_PATH}")
print(f"   - Checkpoints: {CHECKPOINT_PATH}")

# Final counts
raw_count = spark.read.format("delta").load(OUTPUT_PATH).count()
agg_count = spark.read.format("delta").load(AGGREGATED_PATH).count()
anomaly_count = spark.read.format("delta").load(OUTPUT_PATH).filter(col("is_anomaly") == True).count()

print(f"\n📈 Processing Results:")
print(f"   - Total Events Processed: {raw_count:,}")
print(f"   - Aggregation Windows: {agg_count:,}")
print(f"   - Anomalies Detected: {anomaly_count:,}")
print(f"   - Anomaly Rate: {(anomaly_count/raw_count*100):.2f}%")

print("\n" + "=" * 70)
print("Real-time streaming pipeline demonstration completed!")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Production Considerations
# MAGIC 
# MAGIC ### Azure Event Hubs Integration
# MAGIC ```python
# MAGIC # Production Event Hubs configuration
# MAGIC ehConf = {
# MAGIC   'eventhubs.connectionString': sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(CONNECTION_STRING),
# MAGIC   'eventhubs.consumerGroup': "$Default",
# MAGIC   'maxEventsPerTrigger': 1000
# MAGIC }
# MAGIC 
# MAGIC # Read from Event Hubs
# MAGIC event_hubs_stream = spark.readStream \
# MAGIC   .format("eventhubs") \
# MAGIC   .options(**ehConf) \
# MAGIC   .load()
# MAGIC ```
# MAGIC 
# MAGIC ### Key Production Features
# MAGIC 
# MAGIC 1. **Checkpointing**: Enables fault tolerance and exactly-once processing
# MAGIC 2. **Watermarking**: Handles late-arriving data gracefully
# MAGIC 3. **Trigger Intervals**: Balance latency vs throughput
# MAGIC 4. **Auto-scaling**: Databricks auto-scales clusters based on workload
# MAGIC 5. **Monitoring**: Use Databricks streaming UI and metrics
# MAGIC 
# MAGIC ### Alert Integration
# MAGIC ```python
# MAGIC # Send alerts to Azure Logic Apps for anomalies
# MAGIC def send_alert(row):
# MAGIC     if row['is_anomaly']:
# MAGIC         # Call Azure Logic Apps HTTP endpoint
# MAGIC         # Send email/SMS/Teams notification
# MAGIC         pass
# MAGIC ```
# MAGIC 
# MAGIC ### Performance Tuning
# MAGIC - Adjust `maxEventsPerTrigger` for throughput control
# MAGIC - Use `processingTime` trigger for consistent micro-batches
# MAGIC - Optimize Delta Lake with Z-ordering on frequently queried columns
# MAGIC - Monitor checkpoint size and clean up old checkpoints
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC **Portfolio Note:** This notebook demonstrates streaming ETL best practices using Azure Event Hubs and Databricks Structured Streaming. The simulated data stream and simplified architecture are for demonstration purposes.

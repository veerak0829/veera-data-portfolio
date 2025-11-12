# Databricks notebook source
# MAGIC %md
# MAGIC # Batch ETL Pipeline - Customer Transactions Processing
# MAGIC 
# MAGIC ## Overview
# MAGIC This notebook demonstrates a complete batch ETL pipeline using PySpark and Delta Lake.
# MAGIC 
# MAGIC **Pipeline Stages:**
# MAGIC 1. **Bronze Layer**: Ingest raw CSV data with metadata
# MAGIC 2. **Silver Layer**: Clean, validate, and transform data
# MAGIC 3. **Gold Layer**: Create aggregated, business-ready datasets
# MAGIC 
# MAGIC **Data Source:** Customer transaction data (CSV format)
# MAGIC 
# MAGIC **Target:** Delta Lake tables (Bronze → Silver → Gold)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Environment Setup & Configuration

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, input_file_name, lit, 
    to_date, year, month, dayofmonth, sum as _sum,
    count, avg, max as _max, min as _min, round as _round,
    when, regexp_replace, trim, upper, lower,
    row_number, dense_rank
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, DateType, TimestampType
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration Parameters

# COMMAND ----------

# Storage paths (update these with your Azure Data Lake paths)
STORAGE_ACCOUNT = "your_storage_account"
CONTAINER = "data-lake"

# Data paths
RAW_DATA_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw/transactions/"
BRONZE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/bronze/transactions/"
SILVER_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/silver/transactions/"
GOLD_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/gold/transactions_summary/"

# For demonstration, using local paths
RAW_DATA_PATH = "/tmp/raw/transactions/"
BRONZE_PATH = "/tmp/bronze/transactions/"
SILVER_PATH = "/tmp/silver/transactions/"
GOLD_PATH = "/tmp/gold/transactions_summary/"

print("Configuration loaded successfully!")
print(f"Raw Data Path: {RAW_DATA_PATH}")
print(f"Bronze Path: {BRONZE_PATH}")
print(f"Silver Path: {SILVER_PATH}")
print(f"Gold Path: {GOLD_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Define Schema for Raw Data

# COMMAND ----------

# Define schema for incoming CSV data
transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("transaction_date", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("region", StringType(), True),
    StructField("payment_method", StringType(), True)
])

print("Schema defined successfully!")
transaction_schema

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Generate Sample Data (For Portfolio Demonstration)

# COMMAND ----------

# Generate sample transaction data for demonstration
from datetime import datetime, timedelta
import random

# Sample data generator function
def generate_sample_data(num_records=1000):
    """Generate sample transaction data for demonstration"""
    
    categories = ['Electronics', 'Clothing', 'Home & Garden', 'Books', 'Sports', 'Toys']
    products = {
        'Electronics': ['Laptop', 'Smartphone', 'Tablet', 'Headphones', 'Camera'],
        'Clothing': ['T-Shirt', 'Jeans', 'Jacket', 'Shoes', 'Dress'],
        'Home & Garden': ['Furniture', 'Kitchen Appliance', 'Garden Tools', 'Decor'],
        'Books': ['Fiction Novel', 'Technical Book', 'Magazine', 'E-Book'],
        'Sports': ['Running Shoes', 'Yoga Mat', 'Bicycle', 'Tennis Racket'],
        'Toys': ['Action Figure', 'Board Game', 'Puzzle', 'Building Blocks']
    }
    regions = ['North', 'South', 'East', 'West', 'Central']
    payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'Cash', 'Gift Card']
    
    start_date = datetime(2024, 1, 1)
    
    data = []
    for i in range(num_records):
        category = random.choice(categories)
        product = random.choice(products[category])
        transaction_date = start_date + timedelta(days=random.randint(0, 300))
        
        record = (
            f"TXN{i+1:06d}",
            f"CUST{random.randint(1, 500):05d}",
            transaction_date.strftime("%Y-%m-%d"),
            category,
            product,
            random.randint(1, 5),
            round(random.uniform(10.0, 500.0), 2),
            random.choice(regions),
            random.choice(payment_methods)
        )
        data.append(record)
    
    return data

# Generate sample data
sample_data = generate_sample_data(5000)

# Create DataFrame
df_raw = spark.createDataFrame(sample_data, schema=transaction_schema)

print(f"Generated {df_raw.count()} sample transaction records")
df_raw.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Bronze Layer - Raw Data Ingestion

# COMMAND ----------

# Add metadata columns to track data lineage
df_bronze = df_raw \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("sample_data_generator")) \
    .withColumn("bronze_layer_version", lit("v1.0"))

print("Bronze layer data prepared with metadata")
print(f"Total records: {df_bronze.count()}")
df_bronze.show(5)

# Write to Bronze Delta table
df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(BRONZE_PATH)

print(f"✅ Bronze layer created successfully at: {BRONZE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Silver Layer - Data Cleaning & Transformation

# COMMAND ----------

# Read from Bronze layer
df_bronze_read = spark.read.format("delta").load(BRONZE_PATH)

# Data Quality Checks and Transformations
df_silver = df_bronze_read \
    .filter(col("transaction_id").isNotNull()) \
    .filter(col("customer_id").isNotNull()) \
    .filter(col("quantity") > 0) \
    .filter(col("unit_price") > 0) \
    .withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd")) \
    .withColumn("product_category", upper(trim(col("product_category")))) \
    .withColumn("product_name", trim(col("product_name"))) \
    .withColumn("region", upper(trim(col("region")))) \
    .withColumn("payment_method", trim(col("payment_method"))) \
    .withColumn("amount", _round(col("quantity") * col("unit_price"), 2)) \
    .withColumn("tax", _round(col("amount") * 0.1, 2)) \
    .withColumn("total_amount", _round(col("amount") + col("tax"), 2)) \
    .withColumn("year", year(col("transaction_date"))) \
    .withColumn("month", month(col("transaction_date"))) \
    .withColumn("day", dayofmonth(col("transaction_date"))) \
    .withColumn("processing_timestamp", current_timestamp()) \
    .withColumn("data_quality_status", lit("PASSED"))

# Remove duplicates based on transaction_id
df_silver = df_silver.dropDuplicates(["transaction_id"])

print("Silver layer transformations applied")
print(f"Total records after cleaning: {df_silver.count()}")
df_silver.show(5)

# Write to Silver Delta table with partitioning
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .option("overwriteSchema", "true") \
    .save(SILVER_PATH)

print(f"✅ Silver layer created successfully at: {SILVER_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Data Quality Report

# COMMAND ----------

# Data quality metrics
print("=" * 60)
print("DATA QUALITY REPORT")
print("=" * 60)

df_silver_read = spark.read.format("delta").load(SILVER_PATH)

# Count by category
print("\n📊 Transactions by Product Category:")
df_silver_read.groupBy("product_category") \
    .count() \
    .orderBy(col("count").desc()) \
    .show()

# Count by region
print("\n🗺️ Transactions by Region:")
df_silver_read.groupBy("region") \
    .count() \
    .orderBy(col("count").desc()) \
    .show()

# Revenue by payment method
print("\n💳 Revenue by Payment Method:")
df_silver_read.groupBy("payment_method") \
    .agg(
        _round(_sum("total_amount"), 2).alias("total_revenue"),
        count("*").alias("transaction_count")
    ) \
    .orderBy(col("total_revenue").desc()) \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Gold Layer - Business Aggregations

# COMMAND ----------

# Create daily aggregated summary for analytics
df_gold = df_silver_read \
    .groupBy("transaction_date", "region", "product_category") \
    .agg(
        count("transaction_id").alias("transaction_count"),
        _round(_sum("amount"), 2).alias("gross_revenue"),
        _round(_sum("tax"), 2).alias("total_tax"),
        _round(_sum("total_amount"), 2).alias("net_revenue"),
        _round(avg("total_amount"), 2).alias("avg_transaction_value"),
        _round(_min("total_amount"), 2).alias("min_transaction_value"),
        _round(_max("total_amount"), 2).alias("max_transaction_value"),
        count(col("customer_id")).alias("unique_customers")
    ) \
    .withColumn("created_timestamp", current_timestamp())

# Add business metrics
df_gold = df_gold \
    .withColumn("revenue_per_transaction", 
                _round(col("net_revenue") / col("transaction_count"), 2)) \
    .withColumn("avg_tax_rate", 
                _round((col("total_tax") / col("gross_revenue")) * 100, 2))

print("Gold layer aggregations computed")
print(f"Total summary records: {df_gold.count()}")
df_gold.show(10)

# Write to Gold Delta table
df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(GOLD_PATH)

print(f"✅ Gold layer created successfully at: {GOLD_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Business Analytics - Top Insights

# COMMAND ----------

df_gold_read = spark.read.format("delta").load(GOLD_PATH)

# Top performing regions
print("\n🏆 Top 5 Regions by Revenue:")
df_gold_read.groupBy("region") \
    .agg(_round(_sum("net_revenue"), 2).alias("total_revenue")) \
    .orderBy(col("total_revenue").desc()) \
    .limit(5) \
    .show()

# Top performing categories
print("\n📦 Top 5 Product Categories by Revenue:")
df_gold_read.groupBy("product_category") \
    .agg(
        _round(_sum("net_revenue"), 2).alias("total_revenue"),
        _sum("transaction_count").alias("total_transactions")
    ) \
    .orderBy(col("total_revenue").desc()) \
    .limit(5) \
    .show()

# Monthly revenue trend
print("\n📈 Monthly Revenue Trend:")
df_silver_read.groupBy("year", "month") \
    .agg(_round(_sum("total_amount"), 2).alias("monthly_revenue")) \
    .orderBy("year", "month") \
    .show(12)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Performance Optimization

# COMMAND ----------

# Optimize Delta tables for better query performance
print("Optimizing Delta tables...")

# Optimize Bronze table
spark.sql(f"OPTIMIZE delta.`{BRONZE_PATH}`")
print(f"✅ Optimized Bronze table")

# Optimize Silver table with Z-ordering
spark.sql(f"OPTIMIZE delta.`{SILVER_PATH}` ZORDER BY (transaction_date, region)")
print(f"✅ Optimized Silver table with Z-ordering")

# Optimize Gold table
spark.sql(f"OPTIMIZE delta.`{GOLD_PATH}`")
print(f"✅ Optimized Gold table")

print("\nAll Delta tables optimized successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Vacuum Old Files (Cleanup)

# COMMAND ----------

# Clean up old files (retain 7 days of history)
# Note: In production, adjust retention based on requirements

try:
    spark.sql(f"VACUUM delta.`{BRONZE_PATH}` RETAIN 168 HOURS")
    spark.sql(f"VACUUM delta.`{SILVER_PATH}` RETAIN 168 HOURS")
    spark.sql(f"VACUUM delta.`{GOLD_PATH}` RETAIN 168 HOURS")
    print("✅ Vacuum completed - old files cleaned up")
except Exception as e:
    print(f"Note: Vacuum requires Delta table history. Error: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Pipeline Summary

# COMMAND ----------

print("=" * 70)
print("BATCH ETL PIPELINE EXECUTION SUMMARY")
print("=" * 70)
print("\n✅ Pipeline Status: COMPLETED SUCCESSFULLY")
print("\n📊 Data Layers Created:")
print(f"   - Bronze Layer: {BRONZE_PATH}")
print(f"   - Silver Layer: {SILVER_PATH}")
print(f"   - Gold Layer:   {GOLD_PATH}")

# Read final counts
bronze_count = spark.read.format("delta").load(BRONZE_PATH).count()
silver_count = spark.read.format("delta").load(SILVER_PATH).count()
gold_count = spark.read.format("delta").load(GOLD_PATH).count()

print(f"\n📈 Record Counts:")
print(f"   - Bronze Layer: {bronze_count:,} records")
print(f"   - Silver Layer: {silver_count:,} records")
print(f"   - Gold Layer:   {gold_count:,} summary records")

# Calculate total revenue
total_revenue = df_gold_read.agg(_sum("net_revenue")).collect()[0][0]
print(f"\n💰 Total Revenue Processed: ${total_revenue:,.2f}")

print("\n" + "=" * 70)
print("Pipeline execution completed. Data ready for analytics!")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC 1. **Connect Power BI** to Gold layer for visualization
# MAGIC 2. **Schedule this notebook** using Databricks Jobs for daily execution
# MAGIC 3. **Set up alerts** for data quality failures
# MAGIC 4. **Implement incremental processing** using Delta Lake merge operations
# MAGIC 5. **Add data lineage tracking** for compliance and auditing
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC **Portfolio Note:** This notebook demonstrates batch ETL best practices using PySpark and Delta Lake on Azure Databricks. The sample data and paths are for demonstration purposes.

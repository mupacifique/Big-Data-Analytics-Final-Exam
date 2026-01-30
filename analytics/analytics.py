print("Starting Spark Task 3 Analytics ")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, min as spark_min, max as spark_max, count,
    unix_timestamp, to_timestamp
)

# ----------------------------
# 1️⃣ Create Spark Session
# ----------------------------
spark = (
    SparkSession.builder
    .appName("Task3Analytics_Corrected")
    .config("spark.driver.memory", "16g")
    .config("spark.executor.memory", "16g")
    .config("spark.sql.shuffle.partitions", "16")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")
print("✅ Spark session created")

# ----------------------------
# 2️⃣ Load Clean Sessions from ETL
# ----------------------------
input_path = (
    "C:\\Users\\pmuyiringire\\OneDrive - Bank of Kigali\\"
    "BIG DATA ANALYTICS\\SEM 3\\BIG DATA ANALYTICS\\"
    "Assignments\\Final Project\\analytics\\outputs\\sessions.parquet"
)

df = spark.read.parquet(input_path)
print("✅ Sessions parquet loaded successfully")
print("\nSchema:")
df.printSchema()
print("\nSample data:")
df.show(5, truncate=False)
print(f"\nTotal rows: {df.count()}")

# ----------------------------
# 3️⃣ Check Data Structure
# ----------------------------
print("\n🔍 Data structure check:")
print(f"Columns: {df.columns}")
print("\nUnique events in data:")
df.select("event").distinct().show(10, truncate=False)

# ----------------------------
# 4️⃣ Data Validation & Cleaning
# ----------------------------
# Filter out any rows with null user_id, event, or timestamp
df_cleaned = df.filter(
    col("user_id").isNotNull() & 
    col("event").isNotNull() & 
    col("timestamp").isNotNull()
)

# Convert timestamp to proper format if it's a string
# Try multiple timestamp formats

df_cleaned = df_cleaned.withColumn(
    "timestamp_parsed",
    to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss")
)

print(f"✅ Data cleaned: {df_cleaned.count()} rows (removed {df.count() - df_cleaned.count()} null rows)")

# ----------------------------
# 5️⃣ TASK 3A: Conversion Funnel Analysis
# ----------------------------
print("\n" + "="*70)
print("📊 TASK 3A: CONVERSION FUNNEL ANALYSIS")
print("="*70)

# Count events by type - THIS IS YOUR FUNNEL!
funnel_counts = (
    df_cleaned
    .groupBy("event")
    .agg(count("*").alias("count"))
    .orderBy(col("count").desc())
)

print("\n🎯 Funnel Event Counts:")
funnel_counts.show(truncate=False)

# Verify we have the expected events
funnel_list = [row.event for row in funnel_counts.collect()]
expected_events = ['view_item', 'add_to_cart', 'begin_checkout', 'purchase']
missing_events = [e for e in expected_events if e not in funnel_list]

if missing_events:
    print(f"⚠️  WARNING: Missing expected events: {missing_events}")
    print("   Check your data generation script!")
else:
    print("✅ All expected funnel events present")

# ----------------------------
# 6️⃣ TASK 3B: Session Metrics Analysis
# ----------------------------
print("\n" + "="*70)
print("📊 TASK 3B: SESSION METRICS ANALYSIS")
print("="*70)

# Calculate session metrics per user
session_metrics = (
    df_cleaned
    .groupBy("user_id")
    .agg(
        count("*").alias("num_events"),
        spark_min("timestamp_parsed").alias("start_time"),
        spark_max("timestamp_parsed").alias("end_time")
    )
    .withColumn(
        "session_duration_seconds",
        unix_timestamp(col("end_time")) - unix_timestamp(col("start_time"))
    )
    .select(
        "user_id",
        "num_events",
        "start_time",
        "end_time",
        "session_duration_seconds"
    )
    .orderBy(col("session_duration_seconds").desc())
)

print("\n👤 Session Metrics (Top 10 by duration):")
session_metrics.show(10, truncate=False)

print("\n📈 Session Metrics Summary Statistics:")
session_metrics.select(
    "num_events",
    "session_duration_seconds"
).describe().show()

# ----------------------------
# 7️⃣ Save Analytics Outputs
# ----------------------------
output_base = (
    "C:\\Users\\pmuyiringire\\OneDrive - Bank of Kigali\\"
    "BIG DATA ANALYTICS\\SEM 3\\BIG DATA ANALYTICS\\"
    "Assignments\\Final Project\\analytics\\outputs"
)

print("\n💾 Saving analytics outputs...")

# Save funnel counts
funnel_output = f"{output_base}\\funnel_counts.parquet"
funnel_counts.write.mode("overwrite").parquet(funnel_output)
print(f"✅ Funnel counts saved: {funnel_output}")

# Save session metrics
session_output = f"{output_base}\\session_metrics.parquet"
session_metrics.write.mode("overwrite").parquet(session_output)
print(f"✅ Session metrics saved: {session_output}")

# Save cleaned flat data (optional, for reference)
flat_output = f"{output_base}\\sessions_clean.parquet"
df_cleaned.write.mode("overwrite").parquet(flat_output)
print(f"✅ Cleaned sessions saved: {flat_output}")

# ----------------------------
# 8️⃣ Analytics Summary Report
# ----------------------------
print("\n" + "="*70)
print("📈 ANALYTICS SUMMARY REPORT")
print("="*70)

total_events = df_cleaned.count()
total_users = df_cleaned.select("user_id").distinct().count()
unique_event_types = df_cleaned.select("event").distinct().count()

print(f"\n📊 Dataset Overview:")
print(f"  • Total events: {total_events:,}")
print(f"  • Unique users: {total_users:,}")
print(f"  • Event types: {unique_event_types}")
print(f"  • Avg events per user: {total_events / total_users:.2f}")

# Display funnel breakdown with percentages
print(f"\n🎯 Conversion Funnel:")
funnel_data = funnel_counts.collect()
funnel_dict = {row['event']: row['count'] for row in funnel_data}

# Sort by typical funnel order if possible
funnel_order = ['view_item', 'add_to_cart', 'begin_checkout', 'purchase']
sorted_funnel = [(e, funnel_dict[e]) for e in funnel_order if e in funnel_dict]

if sorted_funnel:
    top_count = sorted_funnel[0][1]
    for event, count in sorted_funnel:
        percentage = (count / top_count) * 100
        print(f"  • {event:20s}: {count:10,} ({percentage:6.2f}%)")
    
    # Calculate conversion rates
    if 'view_item' in funnel_dict and 'purchase' in funnel_dict:
        overall_conversion = (funnel_dict['purchase'] / funnel_dict['view_item']) * 100
        print(f"\n💰 Overall Conversion Rate (View → Purchase): {overall_conversion:.2f}%")
else:
    print("  ⚠️  Standard funnel events not found")
    for row in funnel_data:
        print(f"  • {row['event']:20s}: {row['count']:10,}")

# Session metrics summary
session_stats = session_metrics.select(
    "num_events",
    "session_duration_seconds"
).describe().collect()

print(f"\n⏱️  Session Duration Statistics:")
for stat_row in session_stats:
    if stat_row['summary'] in ['mean', 'min', 'max']:
        duration = float(stat_row['session_duration_seconds'])
        print(f"  • {stat_row['summary'].capitalize():10s}: {duration:10.2f} seconds ({duration/60:.2f} minutes)")

print(f"\n📁 Output Files:")
print(f"  1. {funnel_output}")
print(f"  2. {session_output}")
print(f"  3. {flat_output}")

# ----------------------------
# 9️⃣ Stop Spark
# ----------------------------
spark.stop()
print("\n✅ Task 3 Analytics completed successfully!")
print("="*70)

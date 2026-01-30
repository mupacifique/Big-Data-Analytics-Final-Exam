print("Starting Spark ETL job (Memory Optimized)...")

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# ----------------------------
# 1️⃣ Create Spark Session with MORE MEMORY
# ----------------------------
spark = (
    SparkSession.builder
    .appName("FinalProjectETL")
    .config("spark.driver.memory", "16g")           # ← INCREASED from 4g to 8g
    .config("spark.executor.memory", "16g")         # ← INCREASED from 4g to 8g
    .config("spark.memory.fraction", "0.8")        # ← Use 80% of memory for execution
    .config("spark.memory.storageFraction", "0.2") # ← 20% for storage
    .config("spark.sql.shuffle.partitions", "4")   # ← REDUCED partitions (less memory per partition)
    .config("spark.default.parallelism", "16")      # ← Reduce parallelism
    .config("spark.driver.maxResultSize", "16g")    # ← Increase max result size
    .config("spark.sql.files.maxPartitionBytes", "64m")  # ← Smaller file partitions
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")
print("✅ Spark session created")

# ----------------------------
# 2️⃣ Define JSON Schema
# ----------------------------
session_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("event", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# ----------------------------
# 3️⃣ Load JSON files (with memory optimization)
# ----------------------------
input_path = r"C:\Users\pmuyiringire\OneDrive - Bank of Kigali\BIG DATA ANALYTICS\SEM 3\BIG DATA ANALYTICS\Assignments\Final Project\data\raw_events\*.json"

print(f"📂 Loading JSON from: {input_path}")
print("⏳ This may take a while for large files...")

try:
    df = (
        spark.read
        .schema(session_schema)
        .option("mode", "DROPMALFORMED")  # Drop bad records instead of failing
        .json(input_path)
    )
    
    print("✅ JSON files loaded successfully")
    
    # DON'T call count() yet - it triggers computation!
    # Instead, just show a sample
    print("\n📋 Sample data (first 5 rows):")
    df.show(5, truncate=False)
    
except Exception as e:
    print(f"❌ ERROR loading JSON: {e}")
    spark.stop()
    exit(1)

# ----------------------------
# 4️⃣ Write to Parquet (COALESCE to reduce partitions)
# ----------------------------
output_path = r"C:\Users\pmuyiringire\OneDrive - Bank of Kigali\BIG DATA ANALYTICS\SEM 3\BIG DATA ANALYTICS\Assignments\Final Project\analytics\outputs\sessions.parquet"

print(f"\n💾 Writing to Parquet: {output_path}")
print("⏳ Processing and writing data...")

try:
    # Coalesce to fewer partitions to reduce memory pressure
    # Use 2-4 partitions for datasets < 10GB
    df.coalesce(2).write.mode("overwrite").parquet(output_path)
    
    print("✅ Data written to Parquet successfully!")
    
    # Now verify by reading back
    print("\n🔍 Verifying written data...")
    df_verify = spark.read.parquet(output_path)
    row_count = df_verify.count()
    
    print(f"✅ Verification complete: {row_count:,} rows written")
    
    # Show sample of written data
    print("\n📋 Sample from written Parquet:")
    df_verify.show(5, truncate=False)
    
except Exception as e:
    print(f"❌ ERROR writing/verifying data: {e}")
    print("\nIf you still get memory errors, try:")
    print("  1. Reduce the size of your JSON files (use fewer events)")
    print("  2. Process files one at a time")
    print("  3. Increase memory further to 12g or 16g")
    spark.stop()
    exit(1)

# ----------------------------
# 5️⃣ Stop Spark
# ----------------------------
spark.stop()
print("\n✅ Spark ETL job finished successfully!")
print("="*60)
print("Next step: python analytics.py")
print("="*60)

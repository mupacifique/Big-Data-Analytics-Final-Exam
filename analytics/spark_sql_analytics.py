"""
TASK 2: Spark SQL Analytics
Demonstrates complex SQL queries on e-commerce data using Spark SQL.
Simulates querying data from multiple sources (MongoDB/HBase) by loading
different datasets and performing SQL joins and aggregations.
"""

print("="*70)
print("TASK 2: SPARK SQL ANALYTICS")
print("="*70)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum as spark_sum, max as spark_max, min as spark_min

# ----------------------------
# 1️⃣ Initialize Spark Session
# ----------------------------
print("\n🚀 Initializing Spark Session...")

spark = (
    SparkSession.builder
    .appName("Task2_SparkSQL_Analytics")
    .config("spark.driver.memory", "8g")
    .config("spark.executor.memory", "8g")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")
print("✅ Spark session created")

# ----------------------------
# 2️⃣ Load Data from Multiple Sources
# ----------------------------
print("\n📂 Loading data from multiple sources...")
print("   (Simulating data from MongoDB/HBase/distributed storage)")

BASE_PATH = r"C:\Users\pmuyiringire\OneDrive - Bank of Kigali\BIG DATA ANALYTICS\SEM 3\BIG DATA ANALYTICS\Assignments\Final Project"

# Load events data (simulating HBase/Cassandra - time-series events)
events_path = f"{BASE_PATH}\\data\\raw_events\\*.json"
df_events = spark.read.json(events_path)
print(f"   ✅ Events loaded: {df_events.count():,} rows")

# Load users data (simulating MongoDB - user profiles)
users_path = f"{BASE_PATH}\\data\\raw\\users.json"
df_users = spark.read.json(users_path)
print(f"   ✅ Users loaded: {df_users.count():,} rows")

# Load products data (simulating MongoDB - product catalog)
products_path = f"{BASE_PATH}\\data\\raw\\products.json"
df_products = spark.read.json(products_path)
print(f"   ✅ Products loaded: {df_products.count():,} rows")

# Load transactions data (simulating MongoDB - order history)
transactions_path = f"{BASE_PATH}\\data\\raw\\transactions.json"
df_transactions = spark.read.json(transactions_path)
print(f"   ✅ Transactions loaded: {df_transactions.count():,} rows")

# ----------------------------
# 3️⃣ Register DataFrames as SQL Tables
# ----------------------------
print("\n📊 Registering DataFrames as SQL temporary views...")

df_events.createOrReplaceTempView("events")
df_users.createOrReplaceTempView("users")
df_products.createOrReplaceTempView("products")
df_transactions.createOrReplaceTempView("transactions")

print("   ✅ Tables registered: events, users, products, transactions")

# ----------------------------
# 4️⃣ SPARK SQL QUERY 1: Conversion Funnel Analysis
# ----------------------------
print("\n" + "="*70)
print("SQL QUERY 1: CONVERSION FUNNEL ANALYSIS")
print("="*70)

query1 = """
SELECT 
    event,
    COUNT(*) as event_count,
    COUNT(DISTINCT user_id) as unique_users,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM events
GROUP BY event
ORDER BY event_count DESC
"""

print("\n📝 SQL Query:")
print(query1)

result1 = spark.sql(query1)
print("\n📊 Results:")
result1.show(truncate=False)

# ----------------------------
# 5️⃣ SPARK SQL QUERY 2: User Behavior Analysis
# ----------------------------
print("\n" + "="*70)
print("SQL QUERY 2: USER BEHAVIOR ANALYSIS")
print("="*70)

query2 = """
SELECT 
    user_id,
    COUNT(*) as total_events,
    SUM(CASE WHEN event = 'view_item' THEN 1 ELSE 0 END) as views,
    SUM(CASE WHEN event = 'add_to_cart' THEN 1 ELSE 0 END) as add_to_carts,
    SUM(CASE WHEN event = 'purchase' THEN 1 ELSE 0 END) as purchases,
    CASE 
        WHEN SUM(CASE WHEN event = 'purchase' THEN 1 ELSE 0 END) > 0 THEN 'Converter'
        WHEN SUM(CASE WHEN event = 'add_to_cart' THEN 1 ELSE 0 END) > 0 THEN 'Cart Abandoner'
        ELSE 'Browser'
    END as user_segment
FROM events
GROUP BY user_id
ORDER BY total_events DESC
LIMIT 20
"""

print("\n📝 SQL Query:")
print(query2)

result2 = spark.sql(query2)
print("\n📊 Results (Top 20 Users by Activity):")
result2.show(truncate=False)

# ----------------------------
# 6️⃣ SPARK SQL QUERY 3: Geographic Analysis (JOIN with Users)
# ----------------------------
print("\n" + "="*70)
print("SQL QUERY 3: GEOGRAPHIC PURCHASE ANALYSIS (JOIN)")
print("="*70)

query3 = """
SELECT 
    u.geo_data.country as country,
    u.geo_data.state as state,
    COUNT(DISTINCT e.user_id) as active_users,
    SUM(CASE WHEN e.event = 'purchase' THEN 1 ELSE 0 END) as total_purchases,
    ROUND(
        SUM(CASE WHEN e.event = 'purchase' THEN 1 ELSE 0 END) * 100.0 / 
        SUM(CASE WHEN e.event = 'view_item' THEN 1 ELSE 0 END), 2
    ) as conversion_rate_pct
FROM events e
JOIN users u ON e.user_id = u.user_id
WHERE u.geo_data.country IS NOT NULL
GROUP BY u.geo_data.country, u.geo_data.state
HAVING SUM(CASE WHEN e.event = 'view_item' THEN 1 ELSE 0 END) > 100
ORDER BY total_purchases DESC
LIMIT 15
"""

print("\n📝 SQL Query:")
print(query3)

result3 = spark.sql(query3)
print("\n📊 Results (Top 15 Locations by Purchases):")
result3.show(truncate=False)

# ----------------------------
# 7️⃣ SPARK SQL QUERY 4: Product Performance Analysis
# ----------------------------
print("\n" + "="*70)
print("SQL QUERY 4: PRODUCT PERFORMANCE ANALYSIS (SUBQUERY)")
print("="*70)

query4 = """
-- First, flatten transaction items
WITH transaction_items AS (
    SELECT 
        t.transaction_id,
        t.user_id,
        t.timestamp,
        item.product_id,
        item.quantity,
        item.subtotal,
        item.unit_price
    FROM transactions t
    LATERAL VIEW explode(t.items) AS item
)
-- Then aggregate by product
SELECT 
    p.product_id,
    p.name as product_name,
    p.category_id,
    p.base_price,
    COUNT(DISTINCT ti.transaction_id) as num_transactions,
    COALESCE(SUM(ti.quantity), 0) as total_quantity_sold,
    ROUND(COALESCE(SUM(ti.subtotal), 0), 2) as total_revenue,
    ROUND(COALESCE(SUM(ti.subtotal) / NULLIF(SUM(ti.quantity), 0), 0), 2) as avg_sale_price
FROM products p
LEFT JOIN transaction_items ti ON p.product_id = ti.product_id
GROUP BY p.product_id, p.name, p.category_id, p.base_price
HAVING total_revenue > 0
ORDER BY total_revenue DESC
LIMIT 20
"""

print("\n📝 SQL Query:")
print(query4)

result4 = spark.sql(query4)
print("\n📊 Results (Top 20 Products by Revenue):")
result4.show(truncate=False)

# ----------------------------
# 8️⃣ SPARK SQL QUERY 5: Time-based Analysis (Window Functions)
# ----------------------------
print("\n" + "="*70)
print("SQL QUERY 5: DAILY FUNNEL TRENDS (WINDOW FUNCTIONS)")
print("="*70)

query5 = """
WITH daily_events AS (
    SELECT 
        DATE(timestamp) as event_date,
        event,
        COUNT(*) as event_count
    FROM events
    GROUP BY DATE(timestamp), event
)
SELECT 
    event_date,
    event,
    event_count,
    SUM(event_count) OVER (PARTITION BY event ORDER BY event_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as rolling_7day_count,
    LAG(event_count, 1) OVER (PARTITION BY event ORDER BY event_date) as prev_day_count,
    ROUND(
        (event_count - LAG(event_count, 1) OVER (PARTITION BY event ORDER BY event_date)) * 100.0 / 
        NULLIF(LAG(event_count, 1) OVER (PARTITION BY event ORDER BY event_date), 0), 2
    ) as day_over_day_change_pct
FROM daily_events
WHERE event IN ('view_item', 'purchase')
ORDER BY event_date DESC, event
LIMIT 30
"""

print("\n📝 SQL Query:")
print(query5)

result5 = spark.sql(query5)
print("\n📊 Results (Recent Daily Trends):")
result5.show(truncate=False)

# ----------------------------
# 9️⃣ SPARK SQL QUERY 6: Customer Lifetime Value Analysis
# ----------------------------
print("\n" + "="*70)
print("SQL QUERY 6: CUSTOMER LIFETIME VALUE (COMPLEX JOIN)")
print("="*70)

query6 = """
SELECT 
    u.user_id,
    u.geo_data.city as city,
    u.registration_date,
    COUNT(DISTINCT t.transaction_id) as num_orders,
    SUM(t.total) as lifetime_value,
    ROUND(AVG(t.total), 2) as avg_order_value,
    MIN(t.timestamp) as first_purchase,
    MAX(t.timestamp) as last_purchase,
    DATEDIFF(MAX(t.timestamp), MIN(t.timestamp)) as customer_lifespan_days,
    CASE 
        WHEN SUM(t.total) > 1000 THEN 'High Value'
        WHEN SUM(t.total) > 500 THEN 'Medium Value'
        ELSE 'Low Value'
    END as customer_tier
FROM users u
JOIN transactions t ON u.user_id = t.user_id
WHERE t.status = 'completed'
GROUP BY u.user_id, u.geo_data.city, u.registration_date
HAVING COUNT(DISTINCT t.transaction_id) >= 2
ORDER BY lifetime_value DESC
LIMIT 25
"""

print("\n📝 SQL Query:")
print(query6)

result6 = spark.sql(query6)
print("\n📊 Results (Top 25 Customers by Lifetime Value):")
result6.show(truncate=False)

# ----------------------------
# 🔟 Save Query Results
# ----------------------------
print("\n" + "="*70)
print("SAVING QUERY RESULTS")
print("="*70)

output_base = f"{BASE_PATH}\\analytics\\outputs"

# Save each query result as Parquet
result1.write.mode("overwrite").parquet(f"{output_base}\\sql_query1_funnel.parquet")
print("✅ Query 1 results saved: sql_query1_funnel.parquet")

result2.write.mode("overwrite").parquet(f"{output_base}\\sql_query2_user_behavior.parquet")
print("✅ Query 2 results saved: sql_query2_user_behavior.parquet")

result3.write.mode("overwrite").parquet(f"{output_base}\\sql_query3_geographic.parquet")
print("✅ Query 3 results saved: sql_query3_geographic.parquet")

result4.write.mode("overwrite").parquet(f"{output_base}\\sql_query4_product_performance.parquet")
print("✅ Query 4 results saved: sql_query4_product_performance.parquet")

result5.write.mode("overwrite").parquet(f"{output_base}\\sql_query5_daily_trends.parquet")
print("✅ Query 5 results saved: sql_query5_daily_trends.parquet")

result6.write.mode("overwrite").parquet(f"{output_base}\\sql_query6_customer_ltv.parquet")
print("✅ Query 6 results saved: sql_query6_customer_ltv.parquet")

# ----------------------------
# Summary Report
# ----------------------------
print("\n" + "="*70)
print("TASK 2 SUMMARY - SPARK SQL ANALYTICS")
print("="*70)

print("\n✅ Completed SQL Queries:")
print("   1. Conversion Funnel Analysis - Basic aggregation with window functions")
print("   2. User Behavior Segmentation - CASE statements and conditional aggregation")
print("   3. Geographic Analysis - JOIN between events and users tables")
print("   4. Product Performance - Subquery with CTE and complex JOIN")
print("   5. Daily Trends - Window functions (LAG, rolling windows)")
print("   6. Customer Lifetime Value - Multi-table JOIN with date functions")

print("\n📊 Key SQL Features Demonstrated:")
print("   • JOIN operations (INNER, LEFT)")
print("   • Subqueries and CTEs (WITH clause)")
print("   • Window functions (SUM OVER, LAG, PARTITION BY)")
print("   • Aggregations (COUNT, SUM, AVG)")
print("   • CASE statements for conditional logic")
print("   • Date functions (DATE, DATEDIFF)")
print("   • HAVING and WHERE clauses")
print("   • Complex EXPLODE for nested data")

print("\n📁 Simulated Multi-Source Data Integration:")
print("   • Events data (HBase/Cassandra simulation) - Time-series event logs")
print("   • Users data (MongoDB simulation) - User profiles with nested geo_data")
print("   • Products data (MongoDB simulation) - Product catalog with price history")
print("   • Transactions data (MongoDB simulation) - Order history with nested items")

print("\n" + "="*70)
print("✅ TASK 2 COMPLETE!")
print("="*70)

# Stop Spark
spark.stop()

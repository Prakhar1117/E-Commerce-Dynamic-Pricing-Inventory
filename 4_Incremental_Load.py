# Databricks notebook source
from pyspark.sql.functions import col, when, to_date, sum, avg

# 1. Get the new filename from Azure Data Factory
new_file = dbutils.widgets.get("file_name")

# 2. Authenticate
storage_account_name = "adlsdynamicsales1117"
storage_account_key = "YOUR_AZURE_KEY_HERE"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

# 3. Read the new Parquet file
file_path = f"abfss://raw-data@{storage_account_name}.dfs.core.windows.net/{new_file}"
df_new_sales = spark.read.format("parquet").load(file_path)

# 4. Clean the new data (Just like the Silver layer)
df_new_sales = df_new_sales.filter(col("SalesDate") != 'Invalid_Date_String')
df_new_sales = df_new_sales.withColumn("SalesDateOnly", to_date(col("SalesDate"))).dropna(subset=["SalesDateOnly"])

# 5. Read Silver Reference Tables
df_products = spark.table("ecommerce_db.silver_products")
df_inventory = spark.table("ecommerce_db.silver_inventory")

# 6. Apply Dynamic Pricing Logic
df_enriched = df_new_sales.join(df_products, "ProductID", "inner")

# Join with Inventory
df_enriched = df_enriched.withColumn("JoinDate", col("SalesDateOnly").cast("string"))
df_enriched = df_enriched.join(
    df_inventory,
    (df_enriched["ProductID"] == df_inventory["ProductID"]) & 
    (df_enriched["JoinDate"] == df_inventory["InventoryDate"]),
    "left"
).drop(df_inventory["ProductID"]).drop("JoinDate")

# Calculate metrics
df_enriched = df_enriched.withColumn(
    "DynamicUnitPrice",
    when(col("StockLevel") < col("ReorderLevel"), col("BasePrice") * 1.20)
    .when(col("StockLevel") > (col("ReorderLevel") * 2), col("BasePrice") * 0.90)
    .otherwise(col("BasePrice"))
)
df_enriched = df_enriched.withColumn(
    "TotalSaleAmount",
    (col("Quantity") * col("DynamicUnitPrice")) * (1 - col("Discount"))
)

# 7. APPEND to Silver Sales Table
df_enriched.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("ecommerce_db.silver_sales_enriched")

# 8. UPDATE GOLD TABLES (Recalculate Aggregations)
# Read the completely updated Silver table
df_updated_silver_sales = spark.table("ecommerce_db.silver_sales_enriched")

df_gold_sales_daily = df_updated_silver_sales.groupBy("SalesDateOnly").agg(
    sum("TotalSaleAmount").alias("DailyRevenue"),
    sum("Quantity").alias("UnitsSold"),
    avg("DynamicUnitPrice").alias("AverageSellingPrice")
).orderBy("SalesDateOnly", ascending=False)

df_gold_sales_daily.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("ecommerce_db.gold_sales_daily")

print(f"Successfully processed incremental file: {new_file} and updated Gold tables!")
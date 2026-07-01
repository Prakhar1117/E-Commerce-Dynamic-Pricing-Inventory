# Databricks notebook source
from pyspark.sql.functions import sum, avg, col, when

# 1. Read Clean Silver Tables
df_silver_sales = spark.table("ecommerce_db.silver_sales_enriched")
df_silver_inventory = spark.table("ecommerce_db.silver_inventory")

# 2. Create `gold_sales_daily` (Aggregate sales by day)
df_gold_sales_daily = df_silver_sales.groupBy("SalesDateOnly").agg(
    sum("TotalSaleAmount").alias("DailyRevenue"),
    sum("Quantity").alias("UnitsSold"),
    avg("DynamicUnitPrice").alias("AverageSellingPrice")
).orderBy("SalesDateOnly", ascending=False)

# 3. Create `gold_inventory_risk` (Categorize current inventory risk)
df_gold_inventory_risk = df_silver_inventory.withColumn(
    "RiskStatus",
    when(col("StockLevel") <= 0, "OUT OF STOCK")
    .when(col("StockLevel") < col("ReorderLevel"), "HIGH RISK")
    .otherwise("HEALTHY")
)

# 4. Write to Gold Delta Tables (Ready for Power BI)
df_gold_sales_daily.write.format("delta").mode("overwrite").saveAsTable("ecommerce_db.gold_sales_daily")
df_gold_inventory_risk.write.format("delta").mode("overwrite").saveAsTable("ecommerce_db.gold_inventory_risk")

print("Gold Layer built successfully! Ready for Power BI!")
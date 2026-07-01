# Databricks notebook source
from pyspark.sql.functions import col, when, to_date

# 1. Read Raw Bronze Tables
df_sales = spark.table("ecommerce_db.bronze_sales")
df_products = spark.table("ecommerce_db.bronze_products")
df_inventory = spark.table("ecommerce_db.bronze_inventory")

# 2. Data Validation & Cleaning (Rule Enforcement)
# Rule 1: Drop products with no base price
df_products_clean = df_products.dropna(subset=["BasePrice"])

# Rule 2: Fix negative inventory numbers (set them to 0)
df_inventory_clean = df_inventory.withColumn(
    "StockLevel",
    when(col("StockLevel") < 0, 0).otherwise(col("StockLevel"))
)

# Rule 3 & 4: Explicitly filter out corrupt date strings, then convert to Date type
df_sales_clean = df_sales.filter(col("SalesDate") != 'Invalid_Date_String')
df_sales_clean = df_sales_clean.withColumn("SalesDateOnly", to_date(col("SalesDate")))

# 3. Joins & Dynamic Pricing Logic
# Join Sales with Products to get the BasePrice
df_silver_sales = df_sales_clean.join(df_products_clean, "ProductID", "inner")

# Join with Inventory based on ProductID and the specific Date of the sale
# We cast SalesDateOnly to string just for the join to match InventoryDate perfectly
df_silver_sales = df_silver_sales.withColumn("JoinDate", col("SalesDateOnly").cast("string"))

df_silver_sales_enriched = df_silver_sales.join(
    df_inventory_clean,
    (df_silver_sales["ProductID"] == df_inventory_clean["ProductID"]) & 
    (df_silver_sales["JoinDate"] == df_inventory_clean["InventoryDate"]),
    "left"
).drop(df_inventory_clean["ProductID"]).drop("JoinDate")

# Calculate Dynamic Unit Price based on Scarcity
# Rule: If Stock < Reorder, increase price 20%. If Stock > (Reorder*2), drop price 10%.
df_silver_sales_enriched = df_silver_sales_enriched.withColumn(
    "DynamicUnitPrice",
    when(col("StockLevel") < col("ReorderLevel"), col("BasePrice") * 1.20)
    .when(col("StockLevel") > (col("ReorderLevel") * 2), col("BasePrice") * 0.90)
    .otherwise(col("BasePrice"))
)

# Calculate Final Sale Amount (Quantity * Dynamic Price - Discount)
df_silver_sales_enriched = df_silver_sales_enriched.withColumn(
    "TotalSaleAmount",
    (col("Quantity") * col("DynamicUnitPrice")) * (1 - col("Discount"))
)

# 4. Write to Silver Delta Tables
df_silver_sales_enriched.write.format("delta").mode("overwrite").saveAsTable("ecommerce_db.silver_sales_enriched")
df_inventory_clean.write.format("delta").mode("overwrite").saveAsTable("ecommerce_db.silver_inventory")
df_products_clean.write.format("delta").mode("overwrite").saveAsTable("ecommerce_db.silver_products")

print("Silver Layer (Data Validation & Dynamic Pricing) completed successfully!")
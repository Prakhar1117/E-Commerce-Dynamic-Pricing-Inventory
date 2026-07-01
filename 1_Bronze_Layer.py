# Databricks notebook source
# 1. Authenticate (Paste your actual Storage Account Key here!)
storage_account_name = "adlsdynamicsales1117"
storage_account_key = "YOUR_AZURE_KEY_HERE"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

# 2. Create the Database (Clean Slate)
spark.sql("CREATE DATABASE IF NOT EXISTS ecommerce_db")

# 3. Read the New Parquet Files & Save as Delta Tables
base_path = f"abfss://raw-data@{storage_account_name}.dfs.core.windows.net/"

# Sales
df_sales = spark.read.format("parquet").load(base_path + "sales.parquet")
df_sales.write.format("delta").mode("overwrite").saveAsTable("ecommerce_db.bronze_sales")

# Products
df_products = spark.read.format("parquet").load(base_path + "products.parquet")
df_products.write.format("delta").mode("overwrite").saveAsTable("ecommerce_db.bronze_products")

# Customers
df_customers = spark.read.format("parquet").load(base_path + "customers.parquet")
df_customers.write.format("delta").mode("overwrite").saveAsTable("ecommerce_db.bronze_customers")

# Suppliers
df_suppliers = spark.read.format("parquet").load(base_path + "suppliers.parquet")
df_suppliers.write.format("delta").mode("overwrite").saveAsTable("ecommerce_db.bronze_suppliers")

# Inventory
df_inventory = spark.read.format("parquet").load(base_path + "inventory.parquet")
df_inventory.write.format("delta").mode("overwrite").saveAsTable("ecommerce_db.bronze_inventory")

print("Bronze Layer successfully built using Parquet files!")
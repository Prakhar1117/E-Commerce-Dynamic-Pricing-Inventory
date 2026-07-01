# E-Commerce Dynamic Pricing & Inventory Lakehouse

This project is an automated, event-driven Data Lakehouse built on Microsoft Azure. It processes live e-commerce sales, applies dynamic pricing business logic based on real-time inventory levels, and serves data to a Power BI dashboard.

##  Architecture
- **Source:** Python simulator generating live sales data (Parquet format) every 60 seconds.
- **Storage:** Azure Data Lake Storage Gen2 (ADLS Gen2).
- **Orchestration:** Azure Data Factory (Event-based triggers).
- **Compute & Processing:** Azure Databricks (PySpark) using Medallion Architecture.
- **Reporting:** Power BI (DirectQuery).

##  Medallion Architecture (Databricks)
1. **Bronze Layer:** Ingests raw Parquet files from ADLS Gen2 into Delta tables.
2. **Silver Layer:** 
   - Cleans data (handles NULLs, incorrect formats, negative inventory).
   - Joins Sales, Products, and Inventory data.
   - Applies **Dynamic Pricing** logic (increases price by 20% if low stock, decreases by 10% if overstocked).
3. **Gold Layer:** Aggregates data for business reporting (Daily Revenue, Units Sold, Inventory Risk Status).

##  How to Run Locally
1. Install requirements: `pip install pandas numpy pyarrow azure-storage-blob`
2. Run `generate_dataset.py` to create the initial historical Parquet data.
3. Run `auto_live_store.py` to simulate live micro-batches of sales and trigger the Azure Data Factory pipeline.

import pandas as pd
import numpy as np
import time
from datetime import datetime
from azure.storage.blob import BlobServiceClient
import io
import os
from dotenv import load_dotenv

load_dotenv()

# =======================================================
# TODO: FILL IN YOUR AZURE SETTINGS BEFORE RUNNING!
# =======================================================
STORAGE_ACCOUNT_NAME = "adlsdynamicsales1117" # e.g., "adlsdynamicsales1117"
STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_KEY")   # Loads from .env file
CONTAINER_NAME = "raw-data"
# =======================================================

connection_string = f"DefaultEndpointsProtocol=https;AccountName={STORAGE_ACCOUNT_NAME};AccountKey={STORAGE_ACCOUNT_KEY};EndpointSuffix=core.windows.net"

try:
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
except Exception as e:
    print(f"Failed to connect to Azure. Did you fill in your Storage Account Name and Key?\nError: {e}")
    exit()

print("Starting Live E-Commerce Simulator...")
print("Press Ctrl+C to stop the simulator.")

# We need the product/customer IDs to generate valid sales
try:
    products = pd.read_parquet('ecommerce_data/products.parquet')
    customers = pd.read_parquet('ecommerce_data/customers.parquet')
except FileNotFoundError:
    print("Error: Could not find 'ecommerce_data' folder. Make sure you run this script from the correct directory.")
    exit()

sales_id_counter = int(time.time())

while True:
    try:
        # Generate 50-100 random sales for this minute
        num_sales = np.random.randint(5000, 10000)
        current_time = datetime.now()
        
        new_sales = pd.DataFrame({
            'SalesID': range(sales_id_counter, sales_id_counter + num_sales),
            'SalesDate': [current_time.strftime('%Y-%m-%d %H:%M:%S')] * num_sales,
            'ProductID': np.random.choice(products['ProductID'], num_sales),
            'CustomerID': np.random.choice(customers['CustomerID'], num_sales),
            'Quantity': np.random.randint(1, 10, size=num_sales),
            'Discount': np.round(np.random.choice([0.0, 0.05, 0.10], num_sales, p=[0.7, 0.2, 0.1]), 2)
        })
        sales_id_counter += num_sales
        
        # Convert DataFrame to Parquet in memory
        parquet_buffer = io.BytesIO()
        new_sales.to_parquet(parquet_buffer, index=False)
        
        # Upload directly to Azure Data Lake
        filename = f"sales_incremental_{current_time.strftime('%Y%m%d_%H%M%S')}.parquet"
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=filename)
        blob_client.upload_blob(parquet_buffer.getvalue(), overwrite=True)
        
        print(f"[{current_time.strftime('%H:%M:%S')}] Uploaded {num_sales} new sales to Azure (File: {filename})")
        
        # Wait 60 seconds before generating the next batch
        time.sleep(60)
        
    except KeyboardInterrupt:
        print("\nSimulator stopped.")
        break
    except Exception as e:
        print(f"An error occurred during upload: {e}")
        break

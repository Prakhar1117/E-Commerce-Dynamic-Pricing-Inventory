import pandas as pd
import numpy as np
from datetime import datetime
import os
import time

def generate_incremental_data(output_dir="ecommerce_data"):
    """Generates a small batch of incremental data (new sales and inventory updates)."""
    
    print("Generating incremental data batch...")
    
    # We need to read the existing products and customers to ensure referential integrity
    try:
        products = pd.read_parquet(f'{output_dir}/products.parquet')
        customers = pd.read_parquet(f'{output_dir}/customers.parquet')
    except FileNotFoundError:
        print("Error: Initial data not found. Please run generate_dataset.py first.")
        return

    # Generate new sales for "today" (simulating a new login/day)
    num_new_sales = 5000
    current_time = datetime.now()
    
    # Create new Sales IDs starting from a random high number to avoid conflicts with our 1 million records
    start_id = int(time.time()) 
    
    new_sales = pd.DataFrame({
        'SalesID': range(start_id, start_id + num_new_sales),
        'SalesDate': [current_time.strftime('%Y-%m-%d %H:%M:%S')] * num_new_sales,
        'ProductID': np.random.choice(products['ProductID'], num_new_sales),
        'CustomerID': np.random.choice(customers['CustomerID'], num_new_sales),
        'Quantity': np.random.randint(1, 10, size=num_new_sales),
        'Discount': np.round(np.random.choice([0.0, 0.05, 0.10], num_new_sales, p=[0.7, 0.2, 0.1]), 2)
    })
    
    # Save as a new incremental file
    incremental_sales_filename = f'{output_dir}/sales_incremental_{current_time.strftime("%Y%m%d%H%M%S")}.parquet'
    new_sales.to_parquet(incremental_sales_filename, index=False)
    
    print(f"Incremental data generated successfully: {incremental_sales_filename}")
    print("Upload this specific file to your ADLS 'raw-data' container to test incremental loading!")

if __name__ == "__main__":
    generate_incremental_data()

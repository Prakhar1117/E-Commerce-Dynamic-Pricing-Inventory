import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

def generate_ecommerce_data(output_dir="ecommerce_data", num_sales_records=1_000_000):
    """Generates mock data for the E-Commerce Dynamic Pricing project."""
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    print("Generating Suppliers...")
    num_suppliers = 100
    suppliers = pd.DataFrame({
        'SupplierID': range(1, num_suppliers + 1),
        'SupplierName': [f'Supplier_{i}' for i in range(1, num_suppliers + 1)],
        'Country': np.random.choice(['USA', 'UK', 'Germany', 'China', 'India', 'Canada'], num_suppliers)
    })
    suppliers.to_parquet(f'{output_dir}/suppliers.parquet', index=False)

    print("Generating Products...")
    num_products = 5000
    products = pd.DataFrame({
        'ProductID': range(1, num_products + 1),
        'ProductName': [f'Product_{i}' for i in range(1, num_products + 1)],
        'Category': np.random.choice(['Electronics', 'Clothing', 'Home', 'Toys', 'Sports'], num_products),
        'BasePrice': np.round(np.random.uniform(10.0, 500.0), 2),
        'SupplierID': np.random.choice(suppliers['SupplierID'], num_products)
    })
    # Add some nulls to simulate real-world data for validation later
    products.loc[np.random.choice(products.index, 50), 'BasePrice'] = np.nan 
    products.to_parquet(f'{output_dir}/products.parquet', index=False)

    print("Generating Customers...")
    num_customers = 50000
    customers = pd.DataFrame({
        'CustomerID': range(1, num_customers + 1),
        'CustomerName': [f'Customer_{i}' for i in range(1, num_customers + 1)],
        'Segment': np.random.choice(['Consumer', 'Corporate', 'Home Office'], num_customers, p=[0.6, 0.3, 0.1])
    })
    customers.to_parquet(f'{output_dir}/customers.parquet', index=False)

    print("Generating Inventory Data...")
    # Generate inventory records (snapshot for last 30 days)
    days = 30
    start_date = datetime.now() - timedelta(days=days)
    dates = [start_date + timedelta(days=i) for i in range(days)]
    
    inventory_records = []
    # Generate inventory for a subset of products to keep file size manageable, but enough for joins
    for date in dates:
        active_products = np.random.choice(products['ProductID'], size=int(num_products * 0.8), replace=False)
        daily_inv = pd.DataFrame({
            'InventoryDate': date.strftime('%Y-%m-%d'),
            'ProductID': active_products,
            'StockLevel': np.random.randint(0, 1000, size=len(active_products)),
            'ReorderLevel': np.random.randint(50, 200, size=len(active_products))
        })
        inventory_records.append(daily_inv)
        
    inventory = pd.concat(inventory_records, ignore_index=True)
    # Add some negative numbers to test Data Validation rules later
    inventory.loc[np.random.choice(inventory.index, 100), 'StockLevel'] = -50
    inventory.to_parquet(f'{output_dir}/inventory.parquet', index=False)

    print(f"Generating {num_sales_records} Sales Records (This might take a minute)...")
    
    # Generate sales distributed over the last 30 days
    sales_dates = [start_date + timedelta(days=np.random.randint(0, days)) for _ in range(num_sales_records)]
    
    sales = pd.DataFrame({
        'SalesID': range(1, num_sales_records + 1),
        'SalesDate': [d.strftime('%Y-%m-%d %H:%M:%S') for d in sales_dates],
        'ProductID': np.random.choice(products['ProductID'], num_sales_records),
        'CustomerID': np.random.choice(customers['CustomerID'], num_sales_records),
        'Quantity': np.random.randint(1, 10, size=num_sales_records),
        'Discount': np.round(np.random.choice([0.0, 0.05, 0.10, 0.15, 0.20], num_sales_records, p=[0.5, 0.2, 0.15, 0.1, 0.05]), 2)
    })
    
    # Intentionally corrupt some dates for validation checks
    corrupt_indices = np.random.choice(sales.index, 200)
    sales.loc[corrupt_indices, 'SalesDate'] = 'Invalid_Date_String'

    sales.to_parquet(f'{output_dir}/sales.parquet', index=False)
    
    print("Dataset generation complete! Files are located in the 'ecommerce_data' directory.")

if __name__ == "__main__":
    # Generates 1,000,000 sales records and related dimensional tables
    generate_ecommerce_data()

import pandas as pd
import time
import boto3
from decimal import Decimal
import numpy as np
import os
from dotenv import load_dotenv
load_dotenv()

# ==========================================
# 1. CONFIGURATION
# ==========================================
# PASTE YOUR KEYS HERE (Copy them from your previous working script)
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")REGION_NAME = "us-east-2"  # Make sure this matches your Table (e.g., us-east-2)

TABLE_NAME = "Transactions"
FILE_PATH = "Ecomm.csv"

# ==========================================
# 2. DATA PREPARATION (Corrected Logic)
# ==========================================
def clean_record(record):
    """
    Cleans data for DynamoDB.
    CRITICAL FIX: Checks for NaN/Null BEFORE checking for numbers.
    """
    new_record = {}
    
    # List of columns that should definitely be numbers
    numeric_cols = ['Aging', 'Sales', 'Quantity', 'Discount', 'Profit', 'Shipping_Cost']

    for key, value in record.items():
        # --- 1. FORCE CUSTOMER_ID TO STRING ---
        if key == 'Customer_Id':
            new_record[key] = str(value)
            continue
        
        # --- 2. HANDLE NULLS / NANs / INFINITY FIRST ---
        # This prevents the "Infinity and NaN not supported" error
        if pd.isna(value) or value is None:
            if key in numeric_cols:
                new_record[key] = Decimal('0')
            else:
                new_record[key] = "N/A"
            continue
            
        # Check for Infinity specifically (numpy infinity)
        if isinstance(value, float) and np.isinf(value):
             new_record[key] = Decimal('0')
             continue

        # --- 3. HANDLE NUMBERS ---
        if isinstance(value, (int, np.integer)):
            new_record[key] = int(value)
        
        elif isinstance(value, (float, np.floating)):
            # Convert to string first to avoid precision errors
            new_record[key] = Decimal(str(value))
             
        # --- 4. HANDLE EVERYTHING ELSE (Strings) ---
        else:
            new_record[key] = value
            
    return new_record

def prepare_data(file_path):
    print("--- Reading Data ---")
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        print("Error: Ecomm.csv not found.")
        return []

    # Merge Date/Time
    df['Transaction_Date'] = (df['Order_Date'] + ' ' + df['Time'])
    
    # Convert to dictionary list
    raw_data = df.to_dict(orient='records')
    
    # Apply cleaning row by row
    clean_data = [clean_record(row) for row in raw_data]
    
    return clean_data

# ==========================================
# 3. BENCHMARK ENGINE
# ==========================================
def run_benchmark():
    # CONNECT
    print("Connecting to DynamoDB...")
    try:
        dynamodb = boto3.resource(
            'dynamodb',
            region_name=REGION_NAME,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
        table = dynamodb.Table(TABLE_NAME)
        print(f"Connected to table: {table.table_status}")
        print(f"Table ARN: {table.table_arn}")
    except Exception as e:
        print(f"Connection Failed: {e}")
        return

    # LOAD DATA
    full_data = prepare_data(FILE_PATH)
    if not full_data: return
    
    # Define Subsets
    single_insert_data = full_data[:10]
    batch_insert_data = full_data[10:1010] 
    bulk_insert_data = full_data[1010:3010] 
    
    print("\n[Starting DynamoDB Benchmark]")
    latencies = []

    # --- TABLE 1: INSERT ---
    print("\nRunning INSERT Tests...")
    
    # 1. Single Insert
    t_start = time.time()
    for record in single_insert_data:
        op_start = time.time()
        table.put_item(Item=record)
        latencies.append((time.time() - op_start) * 1000)
    t_single = (time.time() - t_start) * 1000
    
    # 2. Batch Insert
    t_start = time.time()
    with table.batch_writer() as batch:
        for record in batch_insert_data:
            batch.put_item(Item=record)
    t_multiple = (time.time() - t_start) * 1000
    
    # 3. Bulk Insert
    t_start = time.time()
    with table.batch_writer() as batch:
        for record in bulk_insert_data:
            batch.put_item(Item=record)
    t_all = (time.time() - t_start) * 1000
    
    print("\n--- TABLE 1: INSERT OPERATION ---")
    print(f"Insert (single)   : {t_single:.3f} ms")
    print(f"Insert (multiple) : {t_multiple:.3f} ms")
    print(f"Insert (all/sim)  : {t_all:.3f} ms")
    print(f"Total             : {t_single + t_multiple + t_all:.3f} ms")

    # --- TABLE 2: READ ---
    print("\nRunning READ Tests...")
    target_id = "37077" # String ID
    
    # 1. Read Specific
    read_latencies = []
    for _ in range(10):
        op_start = time.time()
        table.get_item(Key={'Customer_Id': target_id})
        read_latencies.append((time.time() - op_start) * 1000)
    t_read_specific = sum(read_latencies)
    
    # 2. Read All (Scan Limit)
    op_start = time.time()
    table.scan(Limit=1000)
    t_read_all = (time.time() - op_start) * 1000
    
    latencies.extend(read_latencies)
    
    print("\n--- TABLE 2: READ OPERATION ---")
    print(f"Read (specific) : {t_read_specific:.3f} ms")
    print(f"Read (all)      : {t_read_all:.3f} ms")
    print(f"Total           : {t_read_specific + t_read_all:.3f} ms")

    # --- TABLE 3: UPDATE ---
    print("\nRunning UPDATE Tests...")
    
    # 1. Update Specific
    update_latencies = []
    for _ in range(10):
        op_start = time.time()
        table.update_item(
            Key={'Customer_Id': target_id},
            UpdateExpression="set Order_Priority=:p",
            ExpressionAttributeValues={':p': 'High'}
        )
        update_latencies.append((time.time() - op_start) * 1000)
    t_update_specific = sum(update_latencies)
    
    # 2. Update Many (Loop Simulation)
    t_start = time.time()
    for i in range(10):
        table.update_item(
            Key={'Customer_Id': target_id},
            UpdateExpression="set Discount=:d",
            ExpressionAttributeValues={':d': Decimal('0.5')}
        )
    t_update_many = (time.time() - t_start) * 1000
    
    latencies.extend(update_latencies)
    
    print("\n--- TABLE 3: UPDATE OPERATION ---")
    print(f"Update (specific) : {t_update_specific:.3f} ms")
    print(f"Update (many)     : {t_update_many:.3f} ms")
    print(f"Total             : {t_update_specific + t_update_many:.3f} ms")

    # --- TABLE 4: DELETE ---
    print("\nRunning DELETE Tests...")
    
    # 1. Delete Specific
    delete_latencies = []
    op_start = time.time()
    table.delete_item(Key={'Customer_Id': target_id})
    t_delete_specific = (time.time() - op_start) * 1000
    delete_latencies.append(t_delete_specific)
    
    # 2. Delete Many (Batch)
    t_start = time.time()
    with table.batch_writer() as batch:
        for record in batch_insert_data[:100]:
            batch.delete_item(Key={'Customer_Id': str(record['Customer_Id'])})
    t_delete_many = (time.time() - t_start) * 1000
    
    latencies.extend(delete_latencies)
    
    print("\n--- TABLE 4: DELETE OPERATION ---")
    print(f"Delete (specific) : {t_delete_specific:.3f} ms")
    print(f"Delete (many)     : {t_delete_many:.3f} ms")
    print(f"Total             : {t_delete_specific + t_delete_many:.3f} ms")

    # --- TABLE 5: AGGREGATE ---
    if latencies:
        print("\n--- TABLE 5: AGGREGATE OPERATIONS ---")
        print(f"MIN Value : {min(latencies):.3f} ms")
        print(f"MAX Value : {max(latencies):.3f} ms")
        print(f"AVG Value : {sum(latencies)/len(latencies):.3f} ms")
        print(f"Total     : {sum(latencies):.3f} ms")

    # --- TABLE 6: OVERALL ---
    overall = (t_single + t_multiple + t_all + t_read_specific + t_read_all + 
               t_update_specific + t_update_many + t_delete_specific + t_delete_many)
    print("\n--- TABLE 6: OVERALL PERFORMANCE ---")
    print(f"DynamoDB Total Time Elapsed : {overall:.3f} ms")

if __name__ == "__main__":
    run_benchmark()
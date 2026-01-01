import pandas as pd
import time
from astrapy import DataAPIClient
from decimal import Decimal
import numpy as np
import os
from dotenv import load_dotenv
load_dotenv()

# ==========================================
# 1. CONFIGURATION
# ==========================================
ASTRA_TOKEN = os.getenv("ASTRA_TOKEN")
ASTRA_ENDPOINT = os.getenv("ASTRA_ENDPOINT")

COLLECTION_NAME = "transactions_benchmark"
FILE_PATH = "Ecomm.csv"

# ==========================================
# 2. DATA PREPARATION
# ==========================================
def clean_record(record):
    """
    Cleans data to ensure JSON compatibility.
    """
    new_record = {}
    for key, value in record.items():
        # Force ID to string
        if key == 'Customer_Id':
            new_record[key] = str(value)
            continue
            
        # Handle Nulls/NaNs
        if pd.isna(value) or value is None:
            new_record[key] = None
            continue
            
        if isinstance(value, float) and (np.isinf(value) or np.isnan(value)):
             new_record[key] = None
             continue

        # Standard Types
        if isinstance(value, (int, np.integer)):
            new_record[key] = int(value)
        elif isinstance(value, (float, np.floating)):
            new_record[key] = float(value)
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
    
    # Convert to list of dicts
    raw_data = df.to_dict(orient='records')
    clean_data = [clean_record(row) for row in raw_data]
    return clean_data

# ==========================================
# 3. BENCHMARK ENGINE
# ==========================================
def run_benchmark():
    # CONNECT
    print("Connecting to Astra DB (Data API)...")
    try:
        client = DataAPIClient(ASTRA_TOKEN)
        db = client.get_database_by_api_endpoint(ASTRA_ENDPOINT)
        print(f"Connected! Existing collections: {db.list_collection_names()}")
        
        # --- FIX: ROBUST COLLECTION CREATION ---
        # Instead of using check_exists=False, we check the list manually.
        if COLLECTION_NAME in db.list_collection_names():
            print(f"Collection '{COLLECTION_NAME}' already exists. Using it.")
            collection = db.get_collection(COLLECTION_NAME)
        else:
            print(f"Creating collection '{COLLECTION_NAME}'...")
            collection = db.create_collection(COLLECTION_NAME)
        # ---------------------------------------
        
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
    
    print("\n[Starting Astra DB Benchmark]")
    latencies = []

    # --- TABLE 1: INSERT ---
    print("\nRunning INSERT Tests...")
    
    # Clear collection first
    try:
        collection.delete_many({})
    except:
        pass

    # 1. Single Insert
    t_start = time.time()
    for record in single_insert_data:
        op_start = time.time()
        collection.insert_one(record)
        latencies.append((time.time() - op_start) * 1000)
    t_single = (time.time() - t_start) * 1000
    
    # 2. Batch Insert (Chunked)
    t_start = time.time()
    chunk_size = 20 
    for i in range(0, len(batch_insert_data), chunk_size):
        chunk = batch_insert_data[i:i + chunk_size]
        if chunk:
            collection.insert_many(chunk)
    t_multiple = (time.time() - t_start) * 1000
    
    # 3. Bulk Insert (Chunked)
    t_start = time.time()
    for i in range(0, len(bulk_insert_data), chunk_size):
        chunk = bulk_insert_data[i:i + chunk_size]
        if chunk:
            collection.insert_many(chunk)
    t_all = (time.time() - t_start) * 1000
    
    print("\n--- TABLE 1: INSERT OPERATION ---")
    print(f"Insert (single)   : {t_single:.3f} ms")
    print(f"Insert (multiple) : {t_multiple:.3f} ms")
    print(f"Insert (all/sim)  : {t_all:.3f} ms")
    print(f"Total             : {t_single + t_multiple + t_all:.3f} ms")

    # --- TABLE 2: READ ---
    print("\nRunning READ Tests...")
    target_id = "37077"
    
    # 1. Read Specific
    read_latencies = []
    for _ in range(10):
        op_start = time.time()
        collection.find_one({"Customer_Id": target_id})
        read_latencies.append((time.time() - op_start) * 1000)
    t_read_specific = sum(read_latencies)
    
    # 2. Read All (Limit 1000)
    op_start = time.time()
    # Convert cursor to list
    _ = list(collection.find({}, limit=1000))
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
        collection.update_one(
            {"Customer_Id": target_id},
            {"$set": {"Order_Priority": "High"}}
        )
        update_latencies.append((time.time() - op_start) * 1000)
    t_update_specific = sum(update_latencies)
    
    # 2. Update Many
    t_start = time.time()
    collection.update_many(
        {"Customer_Id": target_id},
        {"$set": {"Discount": 0.5}}
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
    collection.delete_one({"Customer_Id": target_id})
    t_delete_specific = (time.time() - op_start) * 1000
    delete_latencies.append(t_delete_specific)
    
    # 2. Delete Many
    t_start = time.time()
    collection.delete_many({"Order_Priority": "Medium"}) 
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
    print(f"AstraDB Total Time Elapsed : {overall:.3f} ms")

if __name__ == "__main__":
    run_benchmark()
import pandas as pd
import time
import numpy as np
from pymongo import MongoClient

# ==========================================
# 1. CONFIGURATION
# ==========================================

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "EcommDB"
COLLECTION_NAME = "Transactions"
FILE_PATH = "Ecomm.csv"

# ==========================================
# 2. DATA PREPARATION
# ==========================================
def prepare_data(file_path):
    print("--- Reading Data ---")
    df = pd.read_csv(file_path)
    df['Aging'] = df['Aging'].fillna(0)
    df['Sales'] = df['Sales'].fillna(0.0)
    df['Quantity'] = df['Quantity'].fillna(0)
    df['Transaction_Date'] = pd.to_datetime(df['Order_Date'] + ' ' + df['Time'])
    return df.to_dict(orient='records')

# ==========================================
# 3. BENCHMARK ENGINE
# ==========================================
def run_benchmark():
    # CONNECT
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    
    # LOAD DATA
    full_data = prepare_data(FILE_PATH)
    total_records = len(full_data)
    
    # Split data for different tests
    single_insert_data = full_data[:10]       # First 10 for single loops
    batch_insert_data = full_data[10:1010]    # Next 1000 for batch loops
    bulk_insert_data = full_data[1010:]       # The rest (50k+) for "All"
    
    print(f"\n[Starting Advanced Benchmark on {total_records} records]")
    
    # ---------------------------------------------------------
    # TABLE 1: INSERT OPERATIONS
    # ---------------------------------------------------------
    print("\nRunning INSERT Tests...")
    
    # 1. Insert Single (Loop)
    latencies = []
    t_start = time.time()
    for record in single_insert_data:
        op_start = time.time()
        collection.insert_one(record)
        latencies.append((time.time() - op_start) * 1000)
    t_single = (time.time() - t_start) * 1000
    
    # 2. Insert Multiple (Batches of 100)
    batch_size = 100
    batches = [batch_insert_data[i:i + batch_size] for i in range(0, len(batch_insert_data), batch_size)]
    
    t_start = time.time()
    for batch in batches:
        collection.insert_many(batch)
    t_multiple = (time.time() - t_start) * 1000
    
    # 3. Insert All (Bulk)
    # Clear first to treat this as a true "Load"
    collection.delete_many({}) 
    t_start = time.time()
    collection.insert_many(full_data)
    t_all = (time.time() - t_start) * 1000
    
    print("\n--- TABLE 1: INSERT OPERATION ---")
    print(f"Insert (single)   : {t_single:.3f} ms")
    print(f"Insert (multiple) : {t_multiple:.3f} ms")
    print(f"Insert (all)      : {t_all:.3f} ms")
    print(f"Total             : {t_single + t_multiple + t_all:.3f} ms")

    # ---------------------------------------------------------
    # TABLE 2: READ OPERATIONS
    # ---------------------------------------------------------
    print("\nRunning READ Tests...")
    target_id = 37077
    
    # 1. Read Specific (Find One) - Run 10 times to get Avg
    read_latencies = []
    for _ in range(10):
        op_start = time.time()
        _ = collection.find_one({"Customer_Id": target_id})
        read_latencies.append((time.time() - op_start) * 1000)
    
    t_read_specific = sum(read_latencies) # Total time for the 10 reads
    
    # 2. Read All (Find All)
    op_start = time.time()
    # Limiting to 1000 to simulate a "page" read, reading 50k takes too long for display
    _ = list(collection.find().limit(1000)) 
    t_read_all = (time.time() - op_start) * 1000
    
    # Add read latencies to global list for Table 5
    latencies.extend(read_latencies)
    
    print("\n--- TABLE 2: READ OPERATION ---")
    print(f"Read (specific) : {t_read_specific:.3f} ms")
    print(f"Read (all)      : {t_read_all:.3f} ms")
    print(f"Total           : {t_read_specific + t_read_all:.3f} ms")

    # ---------------------------------------------------------
    # TABLE 3: UPDATE OPERATIONS
    # ---------------------------------------------------------
    print("\nRunning UPDATE Tests...")
    
    # 1. Update Specific
    update_latencies = []
    for _ in range(10):
        op_start = time.time()
        collection.update_one({"Customer_Id": target_id}, {"$set": {"Order_Priority": "High"}})
        update_latencies.append((time.time() - op_start) * 1000)
    t_update_specific = sum(update_latencies)
    
    # 2. Update Many
    op_start = time.time()
    collection.update_many({"Order_Priority": "Medium"}, {"$set": {"Order_Priority": "Standard"}})
    t_update_many = (time.time() - op_start) * 1000
    
    latencies.extend(update_latencies)

    print("\n--- TABLE 3: UPDATE OPERATION ---")
    print(f"Update (specific) : {t_update_specific:.3f} ms")
    print(f"Update (many)     : {t_update_many:.3f} ms")
    print(f"Total             : {t_update_specific + t_update_many:.3f} ms")

    # ---------------------------------------------------------
    # TABLE 4: DELETE OPERATIONS
    # ---------------------------------------------------------
    print("\nRunning DELETE Tests...")
    
    # 1. Delete Specific
    delete_latencies = []
    # Insert a dummy record to delete
    collection.insert_one({"Customer_Id": 99999, "Name": "Delete Me"})
    
    op_start = time.time()
    collection.delete_one({"Customer_Id": 99999})
    t_delete_specific = (time.time() - op_start) * 1000
    delete_latencies.append(t_delete_specific)
    
    # 2. Delete Many
    op_start = time.time()
    collection.delete_many({"Order_Priority": "Standard"}) # Deletes the ones we updated earlier
    t_delete_many = (time.time() - op_start) * 1000
    
    latencies.extend(delete_latencies)

    print("\n--- TABLE 4: DELETE OPERATION ---")
    print(f"Delete (specific) : {t_delete_specific:.3f} ms")
    print(f"Delete (many)     : {t_delete_many:.3f} ms")
    print(f"Total             : {t_delete_specific + t_delete_many:.3f} ms")

    # ---------------------------------------------------------
    # TABLE 5: AGGREGATE OPERATIONS (Min/Max/Avg)
    # ---------------------------------------------------------
    # We use the 'latencies' list which collected individual op times
    min_val = min(latencies)
    max_val = max(latencies)
    avg_val = sum(latencies) / len(latencies)
    total_ops_time = sum(latencies)

    print("\n--- TABLE 5: AGGREGATE OPERATIONS ---")
    print(f"MIN Value : {min_val:.3f} ms")
    print(f"MAX Value : {max_val:.3f} ms")
    print(f"AVG Value : {avg_val:.3f} ms")
    print(f"Total     : {total_ops_time:.3f} ms")

    # ---------------------------------------------------------
    # TABLE 6: OVERALL PERFORMANCE
    # ---------------------------------------------------------
    overall_time = (t_single + t_multiple + t_all + 
                    t_read_specific + t_read_all + 
                    t_update_specific + t_update_many + 
                    t_delete_specific + t_delete_many)
                    
    print("\n--- TABLE 6: OVERALL PERFORMANCE ---")
    print(f"MongoDB Total Time Elapsed : {overall_time:.3f} ms")

if __name__ == "__main__":
    run_benchmark()
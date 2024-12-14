import sqlite3

def get_aggregate_resources(db_path, table_name):
    """
    Calculate the aggregate values of core, memory, ssd, and nic from the vmType table.


    """
    # Connect to the database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Query to calculate aggregates
    query = f"""
    SELECT 
        SUM(core) AS total_core,
        SUM(memory) AS total_memory,
        SUM(ssd) AS total_ssd,
        SUM(nic) AS total_nic
    FROM {table_name}
    """
    cursor.execute(query)

    # Fetch the result
    row = cursor.fetchone()

    # Close the connection
    conn.close()

    # Map resources to their aggregate values
    resources_aggregate = {
        "core": row[0] or 0,   # Default to 0 if NULL
        "memory": row[1] or 0,
        "ssd": row[2] or 0,
        "nic": row[3] or 0
    }

    return resources_aggregate



db_path = "/Users/yuelan/Desktop/packing_trace_zone_a_v1.sqlite"
table_name = "vmType"

resources_aggregate = get_aggregate_resources(db_path, table_name)
print(f"Aggregate Resources: {resources_aggregate}")

import json
with open("resource_dict.json", "w") as f:
    json.dump(resources_aggregate, f)


import sqlite3
from collections import defaultdict
from datetime import datetime

'''
    create a task to duration dictionary
'''
def create_task_duration_dicts_by_tenant(db_path, table_name):
    # Connect to the database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Query to fetch tenantId, taskID, starttime, and endtime columns
    query = f"SELECT tenantId, taskID, starttime, endtime FROM {table_name}"
    cursor.execute(query)

    # Fetch all rows
    rows = cursor.fetchall()

    # Close the connection
    conn.close()

    # Create separate dictionaries for each tenant
    tenant_task_durations = defaultdict(dict)

    for tenant_id, task_id, starttime, endtime in rows:
        if starttime is not None and endtime is not None:  # Ensure both times are present
            try:
                # Convert real timestamps to datetime objects
                start = datetime.fromtimestamp(starttime)  # Assuming 'real' is a UNIX timestamp
                end = datetime.fromtimestamp(endtime)
                duration = (end - start).total_seconds()  # Calculate duration in seconds
                tenant_task_durations[tenant_id][task_id] = duration #tenant_id is a key in this dict.
            except ValueError as e:
                # Skip rows with invalid datetime values
                print(f"Skipping task {task_id} for tenant {tenant_id} due to error: {e}")
                continue

    # Convert defaultdict to regular dictionary for cleaner output
    return {tenant: dict(task_durations) for tenant, task_durations in tenant_task_durations.items()}


# Example usage
db_path = "/Users/yuelan/Desktop/packing_trace_zone_a_v1.sqlite"  # Replace with the actual path to your database
table_name = "integrated_with_dependencies_full"  # Replace with your table name

# Create separate dictionaries for each tenant
tenant_task_durations = create_task_duration_dicts_by_tenant(db_path, table_name)


import json

# Save precomputed adjacency lists to a file
with open("duration_dict.json", "w") as f:
    json.dump(tenant_task_durations, f)

# These variables now hold the duration dictionaries for each tenant

import sqlite3
from collections import defaultdict
from datetime import datetime
import json

"""
    Create a task-to-resource and time dictionary
    for each tenant (DAG).
"""

def create_task_resource_lists_by_tenant(db_path, table_name):
    # Connect to the database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Query to fetch necessary columns, including tenantId
    query = f"""
    SELECT tenantId, taskID, starttime, endtime, core, memory, ssd, nic 
    FROM {table_name}
    """
    cursor.execute(query)

    # Fetch all rows
    rows = cursor.fetchall()

    # Close the connection
    conn.close()

    # Create separate lists for each tenant
    tenant_task_resources = defaultdict(list)

    for row in rows:
        tenant_id, task_id, starttime, endtime, core, memory, ssd, nic = row

        # Replace invalid data with 0
        starttime = starttime or 0
        endtime = endtime or 0
        core = core or 0
        memory = memory or 0
        ssd = ssd or 0
        nic = nic or 0

        try:
            # Convert timestamps to datetime objects and calculate duration
            start = datetime.fromtimestamp(starttime) if starttime else None
            end = datetime.fromtimestamp(endtime) if endtime else None
            duration = (end - start).total_seconds() if start and end else 0

            # Build the dictionary for the task
            task_entry = {
                "task": task_id,
                "details": {
                    "duration": duration,
                    "resources": {
                        "core": core,
                        "memory": memory,
                        "ssd": ssd,
                        "nic": nic  # NIC can be null and is replaced with 0
                    }
                }
            }

            # Append the task to the respective tenant's list
            tenant_task_resources[tenant_id].append(task_entry)
        except Exception as e:
            # Print error for debugging purposes
            print(f"Error processing task {task_id} for tenant {tenant_id}: {e}")

    # Convert defaultdict to a regular dictionary for cleaner output
    return {tenant: tasks for tenant, tasks in tenant_task_resources.items()}


# Example usage
db_path = "/Users/yuelan/Desktop/packing_trace_zone_a_v1.sqlite"  # Replace with the actual path to your database
table_name = "integrated_with_dependencies_full"  # Replace with your table name

# Create separate task resource lists for each tenant
tenant_task_duration_resources = create_task_resource_lists_by_tenant(db_path, table_name)

# Save precomputed adjacency lists to a file
with open("duration_resource_dict.json", "w") as f:
    json.dump(tenant_task_duration_resources, f)

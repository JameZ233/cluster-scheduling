import sqlite3
from collections import defaultdict

'''
    Creates an adjacency list.
'''

def create_adjacency_lists_by_tenant(db_path, table_name):
    # Connect to the database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Query to fetch tenantId, taskID, and dependent columns
    query = f"SELECT tenantId, taskID, dependent FROM {table_name}"
    cursor.execute(query)

    # Fetch all rows
    rows = cursor.fetchall()

    # Close the connection
    conn.close()

    # Create adjacency lists for each tenant
    tenant_adjacency_lists = defaultdict(lambda: defaultdict(list))

    for tenant_id, task_id, dependent in rows:
        if dependent:  # If there are dependencies
            dependent_tasks = dependent.split(",")  # Assuming dependencies are comma-separated
            for dep in dependent_tasks:
                tenant_adjacency_lists[tenant_id][dep.strip()].append(task_id)
        else:  # If there are no dependencies, ensure an empty entry
            if task_id not in tenant_adjacency_lists[tenant_id]:
                tenant_adjacency_lists[tenant_id][task_id] = []

    # Convert nested defaultdicts to regular dictionaries for cleaner output
    return {tenant: dict(adj_list) for tenant, adj_list in tenant_adjacency_lists.items()}



db_path = "/Users/yuelan/Desktop/packing_trace_zone_a_v1.sqlite"
table_name = "integrated_with_dependencies_full"

# Create adjacency lists grouped by tenant and store them in a local variable
tenant_adj_lists = create_adjacency_lists_by_tenant(db_path, table_name)

import json

# Save precomputed adjacency lists to a file
with open("adjacency_lists.json", "w") as f:
    json.dump(tenant_adj_lists, f)

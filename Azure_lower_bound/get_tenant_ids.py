import sqlite3

def get_all_tenant_ids(db_path, table_name):
    """
    Retrieve all distinct tenant IDs from the database and store them in a list.

    """
    # Connect to the database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Query to fetch distinct tenant IDs
    query = f"SELECT DISTINCT tenantId FROM {table_name}"
    cursor.execute(query)

    # Fetch all tenant IDs into a list
    tenant_ids = [row[0] for row in cursor.fetchall()]

    # Close the connection
    conn.close()

    return tenant_ids


# Example usage
db_path = "/Users/yuelan/Desktop/packing_trace_zone_a_v1.sqlite"  # Replace with the actual database path
table_name = "integrated_with_dependencies_full"  # Replace with your table name

tenant_ids = get_all_tenant_ids(db_path, table_name)
with open("tenant_ids.txt", "w") as file:
    for tenant_id in tenant_ids:
        file.write(f"{tenant_id}\n")

#tenant chosen: "1439850"
import networkx as nx
import matplotlib.pyplot as plt
import json
import os

def display_and_save_dag(tenant_id, adjacency_list_file, output_dir="./local_output"):
    # Load adjacency lists from the file
    with open(adjacency_list_file, "r") as f:
        tenant_adj_lists = json.load(f)

    # Check if the tenant_id exists in the data
    if tenant_id not in tenant_adj_lists:
        print(f"Tenant ID {tenant_id} not found in the adjacency list.")
        return

    # Get the adjacency list for the selected tenant
    adjacency_list = tenant_adj_lists[tenant_id]

    # Create a directed graph
    graph = nx.DiGraph()

    # Add edges from the adjacency list
    for parent, children in adjacency_list.items():
        for child in children:
            graph.add_edge(parent, child)

    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Draw the graph
    plt.figure(figsize=(10, 8))
    pos = nx.spring_layout(graph)  # Layout for better visualization
    nx.draw(graph, pos, with_labels=True, node_size=3000, node_color="lightblue", font_size=10, font_weight="bold", edge_color="gray")
    plt.title(f"DAG for Tenant ID: {tenant_id}")

    # Save the image locally
    local_path = os.path.join(output_dir, f"tenant_{tenant_id}_dag.png")
    plt.savefig(local_path, format="png", dpi=300)
    plt.close()  # Close the figure to free up memory

    print(f"DAG for Tenant ID {tenant_id} saved locally at {local_path}")

# Example usage
tenant_id_to_visualize = "1439322"
adjacency_list_file = "adjacency_lists.json"

display_and_save_dag(tenant_id_to_visualize, adjacency_list_file)

import json

def count_tasks_in_dag(graph):
    """

    Counts the total number of tasks (nodes) in a DAG.

    """
    return len(graph.keys())

# Load adjacency lists from the JSON file
with open("adjacency_lists.json", "r") as f:
    adjacency_lists = json.load(f)

# Compute total number of tasks for each tenant's DAG
task_counts = {}
for tenant_id, dag in adjacency_lists.items():
    task_counts[tenant_id] = count_tasks_in_dag(dag)


with open("task_counts.json", "w") as f:
    json.dump(task_counts, f)

def find_depth_dag(graph):
    """
    Finds the depth of the DAG using DFS.

    Parameters:
        graph (dict): Adjacency list representation of the DAG, where keys are nodes,
                      and values are lists of neighboring nodes.

    Returns:
        int: The depth of the DAG (length of the longest path).
    """

    def dfs(node):
        # If depth already calculated, return it
        if node in memo:
            return memo[node]

        # Calculate depth as 1 + max depth of all neighbors
        max_depth = 0
        for neighbor in graph.get(node, []):
            max_depth = max(max_depth, dfs(neighbor))

        # Store result and return
        memo[node] = 1 + max_depth
        return memo[node]

    # Memoization dictionary to store depths of nodes
    memo = {}
    max_depth_overall = 0

    # Visit all nodes in the graph (to handle disconnected components)
    for node in graph.keys():
        max_depth_overall = max(max_depth_overall, dfs(node))

    return max_depth_overall


# Example Usage
import json
adjacency_lists = None
with open("adjacency_lists.json", "r") as f:
    adjacency_lists = json.load(f)

depths = {}
for tenant_id, dag in adjacency_lists.items():
    depths[tenant_id] = find_depth_dag(dag)



with open("depths.json", "w") as f:
    json.dump(depths, f)
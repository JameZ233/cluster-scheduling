from collections import deque
import json
import ijson
from multiprocessing import Pool


# Topological sorting for DAG
def topological_sort(graph):
    in_degree = {node: 0 for node in graph}  # Initialize in-degree for each node.
    for u in graph:
        for v in graph[u]:
            in_degree[v] += 1  # Calculate in-degrees.

    zero_in_degree = deque([node for node in graph if in_degree[node] == 0])
    topo_order = []

    while zero_in_degree:
        u = zero_in_degree.popleft()
        topo_order.append(u)
        for v in graph[u]:
            in_degree[v] -= 1
            if in_degree[v] == 0:
                zero_in_degree.append(v)

    return topo_order


# Calculate Critical Path Length (CPLen)
def cplen(graph, durations):
    topo_order = topological_sort(graph)
    cplen_values = {node: 0 for node in graph}

    # Compute the longest path length ending at each node
    for node in reversed(topo_order):
        cplen_values[node] = durations[node] + max((cplen_values[child] for child in graph[node]), default=0)

    return cplen_values


# Calculate TWork for a DAG
def calculate_twork(tasks_resources, resource_capacities):
    max_twork = 0
    for resource, capacity in resource_capacities.items():
        total_demand = sum(
            task["details"]["duration"] * task["details"]["resources"].get(resource, 0)
            for task in tasks_resources
        )
        twork_resource = total_demand / capacity
        max_twork = max(max_twork, twork_resource)
    return max_twork


# Compute Modified Critical Path (ModCP)
def ModCP(graph, stages, durations, capacities, duration_resources=None):
    cplen_dict = cplen(graph, durations)
    modcp = 0

    for stage, tasks in stages.items():
        stage_tasks = [
            task for task in duration_resources if task["task"] in tasks
        ] if duration_resources else []

        twork = calculate_twork(stage_tasks, capacities) if stage_tasks else 0

        if tasks:
            stage_cplen = max(cplen_dict[task] for task in tasks if task in cplen_dict)
        else:
            stage_cplen = 0

        max_twork_cplen = max(twork, stage_cplen)
        min_durations_sum = sum(
            min(durations[task] for task in other_tasks if task in durations)
            for other_stage, other_tasks in stages.items() if other_stage != stage
        )
        modcp_stage = max_twork_cplen + min_durations_sum
        modcp = max(modcp, modcp_stage)

    return modcp

# Construct stages from DAG

def construct_stages(graph):
    # Ensure all neighbors are present as keys in the graph.
    # Now proceed with the original logic
    in_degree = {node: 0 for node in graph}
    for node, neighbors in graph.items():
        for neighbor in neighbors:
            in_degree[neighbor] += 1

    stages = {}
    zero_in_degree = deque([node for node in in_degree if in_degree[node] == 0])
    stage_count = 1

    while zero_in_degree:
        current_stage = [zero_in_degree.popleft() for _ in range(len(zero_in_degree))]
        stages[f"Stage{stage_count}"] = current_stage

        for node in current_stage:
            for neighbor in graph[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    zero_in_degree.append(neighbor)

        stage_count += 1

    return stages


# Process a single DAG
def process_dag(data):
    dag_id, graph, duration, resource_duration, total_resource = data

    all_neighbors = set()
    for node, neighbors in graph.items():
        for neighbor in neighbors:
            all_neighbors.add(neighbor)
    for node in all_neighbors:
        if node not in graph:
            graph[node] = []  # Pad the graph with an empty adjacency list

    # 2. Pad durations for missing nodes
    for node in graph:
        if node not in duration:
            duration[node] = 0  # Assign a default duration if missing

    stages = construct_stages(graph)
    cplen_values = cplen(graph, duration)
    cplen_value = max(cplen_values.values())
    twork_value = calculate_twork(resource_duration, total_resource)
    modcp_value = ModCP(graph, stages, duration, total_resource, resource_duration)
    lower_bound = max(cplen_value, twork_value, modcp_value)

    return {
        "DAG": dag_id,
        "CPLen": cplen_value,
        "TWork": twork_value,
        "ModCP": modcp_value,
        "LowerBound": lower_bound,
    }


# Main Function
def main():


    adjacency_lists = {}

    try:
        with open('adjacency_lists.json', 'r') as f:
            for key, value in ijson.kvitems(f, ""):  # Incrementally load key-value pairs
                adjacency_lists[key] = value  # Append key-value pairs to the dictionary
    except Exception as e:
        print(f"Error while loading adjacency lists: {e}")


    with open('duration_dict.json', 'r') as f:
        durations = json.load(f)
    with open('duration_resource_dict.json', 'r') as f:
        resources = json.load(f)
    with open('tenant_ids.txt', 'r') as f:
        tenant_ids = f.read().splitlines()
    with open('resource_dict.json', 'r') as f:
        resource_sum = json.load(f)

    # Prepare tasks for processing
    tenant = "0"
    tasks = [
        (
            tenant,
            adjacency_lists[tenant],
            durations[tenant],
            resources[tenant],
            resource_sum,
        )
        # for tenant in tenant_ids
    ]

    # Process tasks in parallel
    with Pool(processes=1) as pool:
        results = pool.map(process_dag, tasks)

    # Save results to a JSON file
    with open("results.json", "w") as f:
        json.dump(results, f, indent=4)


if __name__ == "__main__":
    main()

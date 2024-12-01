from venv import create

import pandas as pd
import networkx as nx
from collections import deque
import matplotlib.pyplot as plt

# read csv file
file_path = 'time_duration_SAMPLE.csv'
df = pd.read_csv(file_path)

# create a new graph
G = nx.DiGraph()

def create_sample_graph():
    G = nx.DiGraph()
    G.add_node("S1", duration = 1, resource = 0.000001, task_num = 1)
    G.add_node("S2",duration = 1, resource = 0.000001, task_num = 1)
    G.add_node("S3",duration = 1, resource = 0.000001, task_num = 1)
    G.add_node("S4",duration = 2, resource = 0.1, task_num = 50)
    G.add_node("S5",duration = 1, resource = 0.000001, task_num = 1)
    G.add_node("S6", duration = 10, resource = 0.02, task_num = 1)
    G.add_node("S7",duration = 0.1, resource = 0.9, task_num = 20)
    G.add_node("S8",duration = 1, resource = 0.000001, task_num = 1)

    G.add_edge("S1", "S4")
    G.add_edge("S2", "S4")
    G.add_edge("S3", "S5")
    G.add_edge("S4", "S6")
    G.add_edge("S5", "S6")
    G.add_edge("S6", "S7")
    G.add_edge("S6", "S8")

    return G

testg = create_sample_graph()
# print("Nodes with attributes:", testg.nodes(data=True))
# print("Edges with attributes:", testg.edges(data=True))
def convert_to_ints(str_list):
    int_list = []
    for num in str_list:
        if num.strip():
            try:
                int_list.append(int(num))
            except ValueError:
                print(f"'{num}' is not a valid integer.")
        # else:
        #     print(f"Cannot parse '' to integer.")
    return int_list

# add node
# traverse each row in the df
# Add nodes and edges based on data
for index, row in df.iterrows():
    node_id = row['collection_id']
    time = row['time']

    # Update nodes with time information safely
    if node_id in G:
        node = G.nodes[node_id]
        current_start_time = node.get('start_time', float('inf'))
        current_end_time = node.get('end_time', float('-inf'))
        if time is not None:
            G.nodes[node_id]['start_time'] = min(current_start_time, time)
            G.nodes[node_id]['end_time'] = max(current_end_time, time)
    else:
        G.add_node(node_id, start_time=time, end_time=time)

    # Add edges from parent
    if pd.notna(row['start_after_collection_ids']):
        G.add_edge(row['start_after_collection_ids'], node_id)

    # Add edges from start after collections
    # start_after_ids = row['start_after_collection_ids'].strip('[]').split()
    # start_after_ids = convert_to_ints(start_after_ids)
    # for start_after_id in start_after_ids:
    #     G.add_edge(node_id, start_after_id)

# calculate the t_duration of each node
for node in G.nodes:
    if 'end_time' in G.nodes[node] and 'start_time' in G.nodes[node]:
        G.nodes[node]['duration'] = G.nodes[node]['end_time'] - G.nodes[node]['start_time']
        # print(G.nodes[node]['duration'])
    else:
        # as some nodes may not exist in this part of dataset
        print(f"Warning: Node {node} does not have complete time attributes.")
        G.nodes[node]['duration'] = -1  # this is exception


def calc_critical_path(graph):
    max_duration = 0
    longest_path = []

    for source in graph.nodes:
        # Check single node path (self-loop case)
        if 'duration' in graph.nodes[source]:  # Ensure the 'duration' attribute exists
            current_duration = graph.nodes[source]['duration']
            if current_duration > max_duration:
                max_duration = current_duration
                longest_path = [source]

        # Check paths between distinct source and target nodes
        for target in graph.nodes:
            if source != target and nx.has_path(graph, source, target):
                for path in nx.all_simple_paths(graph, source=source, target=target):
                    current_duration = sum(graph.nodes[node]['duration'] for node in path)
                    if current_duration > max_duration:
                        max_duration = current_duration
                        longest_path = path

    return longest_path, max_duration

def calc_twork(graph):
    # print(graph.nodes)
    twork = 0
    for node in graph.nodes:
        twork += graph.nodes[node]['resource'] * graph.nodes[node]['duration'] * graph.nodes[node]['task_num']

    return twork


def cut_dags(G):
    """
    Function to divide a DAG into smaller subgraphs based on stages.

    Args:
        G (nx.DiGraph): Input directed acyclic graph (DAG).

    Returns:
        list: Ordered list of smaller DAGs.
    """
    # Initialize the result list and processing queue
    L = [G]  # Start with the original graph
    to_process = deque([G])  # Queue for processing

    while to_process:
        G_prime = to_process.pop()  # Pop a graph from the queue

        cut_found = False  # Flag to track if a valid cut is made
        for stage in list(G_prime.nodes):  # Iterate through all nodes (stages)
            # Calculate U(s, G'): unordered neighbors
            unordered_neighbors = get_unordered_neighbors(G_prime, stage)

            # If no unordered neighbors
            if not unordered_neighbors:
                # Cut at the current stage
                G1, G2 = cut_graph(G_prime, stage)

                # Ensure the cut produces two non-empty subgraphs
                if len(G1.nodes) > 0 and len(G2.nodes) > 0:
                    # Replace G' with G1 and G2 in L
                    L.remove(G_prime)
                    L.append(G1)
                    L.append(G2)

                    # Add the new subgraphs to the processing queue
                    to_process.append(G1)
                    to_process.append(G2)

                    cut_found = True  # Mark that a valid cut was found
                    break  # Process the next graph

        # If no valid cut was found, skip further processing
        if not cut_found:
            continue

    return L


def get_unordered_neighbors(G, stage):
    """
    Get unordered neighbors U(s, G) for a given stage in the graph.

    Args:
        G (nx.DiGraph): Input graph.
        stage: Node (stage) in the graph.

    Returns:
        set: Unordered neighbors of the node.
    """
    # Calculate U(s, G) = V - A(s, G) - D(s, G) - {s}
    all_stages = set(G.nodes)
    ancestors = nx.ancestors(G, stage)
    descendants = nx.descendants(G, stage)
    return all_stages - ancestors - descendants - {stage}


def cut_graph(G, stage):
    """
    Cut the graph at a given stage into two subgraphs.

    Args:
        G (nx.DiGraph): Input graph.
        stage: Node (stage) at which to cut.

    Returns:
        tuple: Two subgraphs (G1, G2) after the cut.
    """
    # G1: Subgraph induced by ancestors and the current stage
    ancestors = nx.ancestors(G, stage)
    G1_nodes = ancestors
    G1 = G.subgraph(G1_nodes).copy()

    # G2: Subgraph induced by descendants
    descendants = nx.descendants(G, stage)
    G2_nodes = descendants.union({stage})
    G2 = G.subgraph(G2_nodes).copy()

    return G1, G2

def calc_modcp(graph):
    """
    Calculate the Modified Critical Path (ModCP) for a given graph.

    Args:
        graph (nx.DiGraph): The input DAG.

    Returns:
        float: The ModCP value.
    """
    max_modcp = 0

    # Iterate over all simple paths in the graph
    for source in graph.nodes:
        for target in graph.nodes:
            if source != target and nx.has_path(graph, source, target):
                for path in nx.all_simple_paths(graph, source=source, target=target):
                    # Initialize variables for this path
                    max_stage_value = 0

                    # print(f"total work for {path}: {total_twork}")
                    for stage in path:
                        # Compute CPLen_s (Critical Path Length starting at stage)
                        subgraph = graph.subgraph(path[path.index(stage):])  # Subgraph from current stage onward
                        _, cplen_s = calc_critical_path(graph.subgraph(stage))
                        stage_twork = calc_twork(graph.subgraph(stage))
                        print(f"cplength for stage:{stage} is {cplen_s}")
                        # Compute min_t_duration for tasks not in the current stage
                        remaining_nodes = set(path) - {stage}
                        min_t_duration = sum(graph.nodes[node]['duration'] for node in remaining_nodes if 'duration' in graph.nodes[node])
                        print(f"min_t_duration: {min_t_duration} despite stage: {stage}")
                        # Compute the stage value
                        stage_value = max(stage_twork, cplen_s) + min_t_duration
                        print(f"stage_value: {stage_value}")
                        max_stage_value = max(max_stage_value, stage_value)

                    # Update max_modcp for this path
                    max_modcp = max(max_modcp, max_stage_value)

    return max_modcp


G1, G2 = cut_dags(create_sample_graph())
print(calc_twork(G1))
print(calc_twork(G2))



def cal_newlb(graph):
    G1, G2 = cut_dags(graph)
    print(G1.nodes)
    print(G2.nodes)
    _, G1_CP = calc_critical_path(G1)
    print(G1_CP)
    _, G2_CP = calc_critical_path(G2)
    print(G2_CP)
    G1_TW = calc_twork(G1)
    print(G1_TW)
    G2_TW = calc_twork(G2)
    print(G2_TW)
    G1_ModCP = calc_modcp(G1)
    print(G1_ModCP)
    G2_ModCP = calc_modcp(G2)
    print(G2_ModCP)
    new_lb = max(G1_CP, G1_TW, G1_ModCP) + max(G2_CP, G2_TW, G2_ModCP)

    return new_lb


print(cal_newlb(create_sample_graph()))
import networkx as nx
from collections import deque

def create_sample_graph():
    """
    Create a sample graph exactly the same as the graph in the paper.
    :return: graph
    """
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


def calc_critical_path(graph):
    """
    Calculate the critical path in the diagram and its total duration.

    Args:
        graph (nx.Graph): a NetworkX diagram which nodes have 'duration' property，represents the duration of this task.

    Returns:
        tuple: (longest_path, max_duration)
            - longest_path (list): The path with the longest duration in the figure (list of nodes included)
            - max_duration (int): Total duration of the critical path
    """
    max_duration = 0
    longest_path = []

    for source in graph.nodes:
        # check self-loop case
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
    """
    Calculate the Total Work of the graph.
    :param graph: a NetworkX diagram with 'resource', 'duration' and 'task_num' properties.
    :return: Total work (int)
    """
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
                    # Replace G with G1 and G2 in L
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
    Formula: U(s, G) = V - A(s, G) - D(s, G) - {s}

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
    Formula: Please reference to the paper
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
                    max_stage_value = 0
                    for stage in path:
                        # the cplenth of the selected stage
                        _, cplen_s = calc_critical_path(graph.subgraph(stage))
                        stage_twork = calc_twork(graph.subgraph(stage))
                        # min_t_duration for tasks except for the current stage
                        remaining_nodes = set(path) - {stage}
                        min_t_duration = sum(graph.nodes[node]['duration'] for node in remaining_nodes if 'duration' in graph.nodes[node])
                        # Compute the stage value
                        stage_value = max(stage_twork, cplen_s) + min_t_duration
                        max_stage_value = max(max_stage_value, stage_value)

                    max_modcp = max(max_modcp, max_stage_value)

    return max_modcp


def calc_newlb(graph):
    """
    Calculate the NewLB for the graph.
    Formula: NewLB = SUM(max(CPLen_s, TWork_s, ModCP_s)+ SUM(duration_expect_s))
    :param graph:
    :return:
    """
    G1, G2 = cut_dags(graph)
    _, G1_CP = calc_critical_path(G1)
    _, G2_CP = calc_critical_path(G2)
    G1_TW = calc_twork(G1)
    G2_TW = calc_twork(G2)
    G1_ModCP = calc_modcp(G1)
    G2_ModCP = calc_modcp(G2)
    new_lb = max(G1_CP, G1_TW, G1_ModCP) + max(G2_CP, G2_TW, G2_ModCP)

    return new_lb


graph = create_sample_graph()
NewLB = calc_newlb(graph)
print(NewLB)


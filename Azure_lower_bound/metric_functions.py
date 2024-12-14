from collections import deque



def topological_sort(graph):

    in_degree = {node: 0 for node in graph}  # Initialize in-degree for each node.
    for u in graph:
        for v in graph[u]:
            in_degree[v] += 1  # Calculate in-degrees.

    # Nodes with no incoming edges can be processed first.
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


def cplen(graph, durations):
    """
    Calculate the critical path length (CPLen) of a Directed Acyclic Graph (DAG)
    using dynamic programming.

    """
    # Perform topological sorting.
    topo_order = topological_sort(graph)

    # Initialize a dictionary to store CPLen for each node.
    cplen = {node: 0 for node in graph}

    # Traverse nodes in reverse topological order to compute CPLen.
    for node in reversed(topo_order):
        # CPLen is the task duration of the current node plus the maximum CPLen of its children.
        cplen[node] = durations[node] + max((cplen[child] for child in graph[node]), default=0)

    return cplen

def Twork(tasks, capacities):
    """
    Calculate Total Work (TWork_G) for a set of tasks and resource capacities.
    """
    total_work = 0  # Initialize total work

    for resource, capacity in capacities.items():
        if capacity == 0:
            continue  # Avoid division by zero

        # Compute the total work for this resource
        resource_work = sum(task['duration'] * task['resources'].get(resource, 0) for task in tasks) / capacity #sum up all the work of the resource
        total_work = max(total_work, resource_work)  # Update maximum workload

    return total_work




def ModCP(graph, stages, durations, capacities):
    """
    Calculate ModCP_G for a Directed Acyclic Graph (DAG) with multiple stages.

    """

    # Step 1: Compute CPLen for each node
    cplen_dict = cplen(graph, durations)

    # Step 2: Initialize ModCP value
    modcp = 0

    # Step 3: Iterate through all paths in the DAG
    for stage, tasks in stages.items():  # Iterate through each stage
        # Step 3.1: Compute TWork for the current stage
        stage_tasks = [{'duration': durations[task], 'resources': task_demands[task]} for task in tasks]
        twork = Twork(stage_tasks, capacities)

        # Step 3.2: Calculate max(TWork, CPLen) for the current stage
        max_twork_cplen = max(twork, cplen_dict[stage])

        # Step 3.3: Compute the sum of minimum task durations for all other stages
        min_durations_sum = 0
        for other_stage, other_tasks in stages.items():
            if other_stage != stage:  # Skip the current stage
                min_durations_sum += min(durations[task] for task in other_tasks)

        # Step 3.4: Calculate ModCP for the current stage
        modcp_stage = max_twork_cplen + min_durations_sum

        # Step 3.5: Update ModCP with the maximum value across all stages
        modcp = max(modcp, modcp_stage)

    return modcp


#construct stages based on a graph.
def construct_stages(graph):
    """
    Construct stages from a Directed Acyclic Graph (DAG) represented as an adjacency list.

    """
    from collections import deque

    # Step 1: Calculate in-degree for each node
    in_degree = {node: 0 for node in graph}
    for node, neighbors in graph.items():
        for neighbor in neighbors:
            in_degree[neighbor] += 1

    # Step 2: Initialize stages and a queue for nodes with zero in-degree
    stages = {}
    zero_in_degree = deque([node for node in in_degree if in_degree[node] == 0])
    stage_count = 1

    # Step 3: Process nodes stage by stage
    while zero_in_degree:
        current_stage = []  # Nodes in the current stage

        # Process all zero in-degree nodes
        for _ in range(len(zero_in_degree)):
            node = zero_in_degree.popleft() #if its zero, then (you know if you are zero at the start) you
                                            #are already at ready to run (you don't depend on anybody)
                                            #and when you are added to a stage, we know you'd be run before other nodes
                                            #as this stage is sequentially prior. so we decrease the in-degree
                                            #of nodes depending on it by 1.

                                            #for nodes that started not in zero, once all the nodes they depend on
                                            #gets staged,their own in-degree would be reduced to 0, and thus they are stageable.

            current_stage.append(node)

            # Remove the node by decrementing in-degree of its neighbors
            for neighbor in graph[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    zero_in_degree.append(neighbor)

        # Add the current stage to the stages dictionary
        stages[f"Stage{stage_count}"] = current_stage
        stage_count += 1

    return stages





import json

def main():
    # dags: 77, 87, 97:
    '''
    #the objective is to use cplen, twork, and modcp for each dag to complete the computation.
    #the functions are written, and so are the helper functions.

    #trouble is that there is upward of 5 million rows of data. which the sql component could not finish
    #in time before our deadline


    #this means that there are discrepencies between the duration list and graph, in our attempt
    # to work only on the dags that we can find, which are both used by cplen, twork, and modcp.
    hence the data endedup being incomplete at the end.

    #given more time, to compute, this logic can yield lower bound estimate of azure
    '''
    task_dependents = None #nodes to whom the nodes (keys) are parents
    task_duration_resource = None#duration and resource for tasks by their dag
    task_duration = None #duration for tasks by their dag (tenant

    with open("adjacency_lists.json", "r") as f:
        adjacency_lists = json.load(f)

    with open("duration_dict.json", "r") as f:
        task_duration = json.load(f)

    with open("duration_resource_dict.json", "r") as f:
        task_duration_resource = json.load(f)

    cplen_77.


if __name__ == '__main__':
    main()


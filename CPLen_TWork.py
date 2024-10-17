import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt

# read csv file
file_path = 'time_duration_SAMPLE.csv'
df = pd.read_csv(file_path)

# create a new graph
G = nx.DiGraph()

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
    if pd.notna(row['parent_collection_id']):
        G.add_edge(row['parent_collection_id'], node_id)

    # Add edges from start after collections
    start_after_ids = row['start_after_collection_ids'].strip('[]').split()
    start_after_ids = convert_to_ints(start_after_ids)
    for start_after_id in start_after_ids:
        G.add_edge(node_id, start_after_id)


######################## CPLen ########################
# calculate the t_duration of each node
for node in G.nodes:
    if 'end_time' in G.nodes[node] and 'start_time' in G.nodes[node]:
        G.nodes[node]['duration'] = G.nodes[node]['end_time'] - G.nodes[node]['start_time']
        # print(G.nodes[node]['duration'])
    else:
        # as some nodes may not exist in this part of dataset
        print(f"Warning: Node {node} does not have complete time attributes.")
        G.nodes[node]['duration'] = -1  # this is exception

max_duration = 0
longest_path = []
# traverse all nodes
for source in G.nodes:
    for target in G.nodes:
        if nx.has_path(G, source, target):
            for path in nx.all_simple_paths(G, source=source, target=target):
                current_duration = sum(G.nodes[node]['duration'] for node in path)
                if current_duration > max_duration:
                    max_duration = current_duration
                    longest_path = path

print("Longest path:", longest_path)
print("Duration of longest path(CPLen):", max_duration)
######################## End of CPLen ########################


######################## TWork ###############################
# read csv file
file_path = 'instance_usage_SAMPLE.csv'
df = pd.read_csv(file_path)
# print(df.head(10))
for index, row in df.iterrows():
    node_id = row['collection_id']
    # Update nodes with time information safely
    if node_id in G:
        G.nodes[node_id]['cpu_usage'] = row['avg_cpu_usage']
        G.nodes[node_id]['memory_usage'] = row['avg_memory_usage']
        # print(G.nodes[node_id])

TWork_CPU = 0
TWork_MEM = 0
for node in G.nodes:
    TWork_CPU += G.nodes[node]['cpu_usage']*G.nodes[node]['duration']
    TWork_MEM += G.nodes[node]['memory_usage']*G.nodes[node]['duration']


TWork = max(TWork_CPU, TWork_MEM)
print("TWork:", f"{TWork_MEM:.0f}")
######################## End of TWork ###############################
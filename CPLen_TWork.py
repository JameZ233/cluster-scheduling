import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt

# read csv file
file_path = 'my_data.csv'
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
for index, row in df.iterrows():

    node_id = row['collection_id']
    time = row['time']

    if node_id in G:
        # if this node exists, then update by the newest start_time or end_time or no update
        node = G.nodes[node_id]
        node['start_time'] = min(node.get('start_time'), time)
        node['end_time'] = max(node.get('end_time'), time)
    else:
        # if this is new, create a new node
        G.add_node(node_id, start_time=time, end_time=time)

    # WARNING: some child node (start_after_id_collection) may not exist in
    # this dataset, as it may not be record in this trace dataset.

    if pd.notna(row['parent_collection_id']):
        G.add_edge(row['parent_collection_id'], row['collection_id'])
        # print(row['parent_collection_id'])

    if pd.notna(row['start_after_collection_ids']) and row['start_after_collection_ids'] != '':
        start_after_ids = row['start_after_collection_ids'].strip('[]').split(' ')
        start_after_ids = convert_to_ints(start_after_ids)
        if(len(start_after_ids) != 0):
            for start_after_id in start_after_ids:
                G.add_edge(row['collection_id'], start_after_id)


print("Successors of A:", list(G.successors(396023520390)))
print("Predecessors of A:", list(G.predecessors(396023520390)))



# calculate the t_duration of each node
for node in G.nodes:
    if 'end_time' in G.nodes[node] and 'start_time' in G.nodes[node]:
        G.nodes[node]['duration'] = G.nodes[node]['end_time'] - G.nodes[node]['start_time']
        print(G.nodes[node]['duration'])
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
print("Duration of longest path:", max_duration)








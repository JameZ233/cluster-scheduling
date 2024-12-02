import pandas as pd
import networkx as nx
import ast
import matplotlib.pyplot as plt

from NewLB_Example import calc_newlb, calc_critical_path, calc_twork, cut_dags, graph

file_path = 'SAMPLE_combination.csv'
df = pd.read_csv(file_path)

G = nx.DiGraph()

for index, row in df.iterrows():
    # if this node does not exist, create it
    if not G.has_node(row['collection_id']):
        G.add_node(row['collection_id'], duration=None, resource=None, task_num=None)

    # update the unknown node
    G.nodes[row['collection_id']].update({
        'duration': row['time_duration'],
        'resource': max(row['avg_cpu_usage'], row['avg_memory_usage']),
        'task_num': row['record_count']
    })

    if not pd.isna(row['start_after_collection_ids']):
        try:
            # as the original structure does not have comma, so we replace the blank with comma first
            fixed_ids = row['start_after_collection_ids'].replace(" ", ",")
            fixed_ids = fixed_ids if fixed_ids.startswith("[") else f"[{fixed_ids}]"
            # parse to list
            parent_ids = ast.literal_eval(fixed_ids)
            for parent_id in parent_ids:
                if not G.has_node(parent_id):
                    G.add_node(parent_id, duration=None, resource=None, task_num=None)
                G.add_edge(parent_id, row['collection_id'])
        except (ValueError, SyntaxError) as e:
            print(f"Error parsing start_after_collection_ids for row {index}: {row['start_after_collection_ids']}")

# summarize the information of this graph
print(f"Graph has {G.number_of_nodes()} nodes and {G.number_of_edges()} edges.")

# we separate this graph into many subgraphs that its nodes are connected
subgraphs = [G.subgraph(c).copy() for c in nx.weakly_connected_components(G)]

for i, subgraph in enumerate(subgraphs):
    newlb = calc_newlb(subgraph)
    _, cplen = calc_critical_path(subgraph)
    twork = calc_twork(subgraph)

    # if the NewLB works for this subgraph, that is NewLB is greater than both CPlen and TWork
    if newlb != -1 and newlb > cplen and newlb > twork:
        print(f"----------Works for {i}----------")
        print(f"NewLB: {newlb}")
        print(f"CPLen: {cplen}")
        print(f"TWork: {twork}")
        graph1, graph2 = cut_dags(subgraph)
        print(f"Graph1: {graph1.nodes}")
        print(f"Graph2: {graph2.nodes}")

        # visualize
        plt.figure(figsize=(15, 15))

        pos = nx.spring_layout(subgraph)

        nx.draw(subgraph, pos, with_labels=True, node_size=500, font_size=12, alpha=0.7)

        labels = {node: f"{node}\n{attr}" for node, attr in nx.get_node_attributes(subgraph, 'duration').items()}
        for node in subgraph.nodes(data=True):
            attributes = ', '.join(f"{key}={value}" for key, value in node[1].items())
            labels[node[0]] = f"{node[0]}\n{attributes}"

        nx.draw_networkx_labels(subgraph, pos, labels=labels, font_size=8)
        plt.xlim(-1.5, 1.5)
        plt.ylim(-1.5, 1.5)

        plt.suptitle(f"Subgraph {i}", fontsize=16)
        plt.show()


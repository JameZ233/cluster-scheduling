import networkx as nx
import matplotlib.pyplot as plt

# 示例数据，你需要用实际从文件或数据库中读取的数据替换这部分
data = [
    {'time': '2021-01-01', 'type': 'Init', 'collection_id': 'A', 'parent_collection_id': 'None', 'start_after_collection_ids': []},
    {'time': '2021-01-02', 'type': 'Process', 'collection_id': 'B', 'parent_collection_id': 'A', 'start_after_collection_ids': []},
    {'time': '2021-01-03', 'type': 'Process', 'collection_id': 'C', 'parent_collection_id': 'A', 'start_after_collection_ids': ['B']},
    {'time': '2021-01-04', 'type': 'Finalize', 'collection_id': 'D', 'parent_collection_id': 'B', 'start_after_collection_ids': ['C']}
]

# 创建图实例
G = nx.DiGraph()

# 基于数据添加节点和边
for item in data:
    # 添加节点
    G.add_node(item['collection_id'])

    # 添加基于 parent_collection_id 的边，如果不是 'None'
    if item['parent_collection_id'] != 'None':
        G.add_edge(item['parent_collection_id'], item['collection_id'])

    # 添加基于 start_after_collection_ids 的边
    for start_after_id in item['start_after_collection_ids']:
        G.add_edge(start_after_id, item['collection_id'])

# 检查是否有环并尝试拓扑排序
if nx.is_directed_acyclic_graph(G):
    print("Topological Sort:", list(nx.topological_sort(G)))
else:
    print("The graph has cycles and cannot be topologically sorted.")

nx.draw(G, with_labels=True)
plt.show()
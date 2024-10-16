import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt

# 读取 CSV 文件
file_path = 'my_data.csv'  # 替换为你的 CSV 文件路径
df = pd.read_csv(file_path)

print(df.head(100))
# 创建一个有向图
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

# 添加节点和边
for index, row in df.iterrows():
    # 添加节点
    G.add_node(row['collection_id'])

    # 解析并处理 parent_collection_id 字段，如果不为空且不为'None'
    if pd.notna(row['parent_collection_id']):
        G.add_edge(row['parent_collection_id'], row['collection_id'])
        # print(row['parent_collection_id'])

    if pd.notna(row['start_after_collection_ids']) and row['start_after_collection_ids'] != '':
        start_after_ids = row['start_after_collection_ids'].strip('[]').split(' ')
        start_after_ids = convert_to_ints(start_after_ids)
        if(len(start_after_ids) != 0):
            for start_after_id in start_after_ids:
                G.add_edge(row['collection_id'], start_after_id)



import networkx as nx

# 假设 G 是已经创建并填充的 NetworkX 有向图 (DiGraph)

# 用来存储既有父节点也有子节点的节点
nodes_with_parents_and_children = []

# 遍历图中的所有节点
for node in G.nodes():
    # 获取每个节点的前驱和后继
    predecessors = list(G.predecessors(node))  # 父节点列表
    successors = list(G.successors(node))      # 子节点列表

    # 检查节点是否既有前驱也有后继
    if predecessors and successors:
        nodes_with_parents_and_children.append(node)

# 打印结果
print("Nodes with both parents and children:", nodes_with_parents_and_children)
# For node 'A'
print("Successors of A:", list(G.successors(377442381085)))
print("Predecessors of A:", list(G.predecessors(377442381085)))


central_nodes = [node for node in G.nodes() if list(G.predecessors(node)) and list(G.successors(node))]
peripheral_nodes = [node for node in G.nodes() if node not in central_nodes]

# 创建一个新的有向图
H = nx.DiGraph(G)  # 创建一个与 G 相同的图

# 使用spring布局作为基础
pos = nx.spring_layout(H)

# 调整节点位置
# 集中 central_nodes，将 peripheral_nodes 推向边缘
for node in pos:
    if node in central_nodes:
        # 向图的中心拉近
        pos[node] = (pos[node][0] * 0.5, pos[node][1] * 0.5)
    else:
        # 推向边缘
        pos[node] = (pos[node][0] * 1.5, pos[node][1] * 1.5)

# 可视化图
plt.figure(figsize=(12, 12))
nx.draw(H, pos, with_labels=True, node_color='lightblue', node_size=500, font_size=9, font_weight='bold', alpha=0.7)
plt.title('Graph Visualization with Central and Peripheral Nodes')
plt.show()


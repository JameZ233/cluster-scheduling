import pandas as pd
import networkx as nx
import ast  # 用于解析字符串形式的列表
import NewLB_Example
import matplotlib.pyplot as plt

from NewLB_Example import calc_newlb, calc_critical_path, calc_twork, cut_dags, graph

# 读取 CSV 文件
file_path = 'SAMPLE_combination (5).csv'  # 替换为你的文件路径
df = pd.read_csv(file_path)

# 创建一个有向图
G = nx.DiGraph()

# 遍历 DataFrame，添加节点和边
for index, row in df.iterrows():
    # 如果节点尚未存在，先添加节点（空属性占位）
    if not G.has_node(row['collection_id']):
        G.add_node(row['collection_id'], duration=None, resource=None, task_num=None)

    # 更新节点属性
    G.nodes[row['collection_id']].update({
        'duration': row['time_duration'],
        'resource': max(row['avg_cpu_usage'], row['avg_memory_usage']),
        'task_num': row['record_count']
    })

    # 解析 start_after_collection_ids 字段
    if not pd.isna(row['start_after_collection_ids']):  # 检查是否为空
        try:
            # 预处理: 替换空格为逗号，确保格式合法
            fixed_ids = row['start_after_collection_ids'].replace(" ", ",")
            # 确保两端有括号（如果没有括号需要添加）
            fixed_ids = fixed_ids if fixed_ids.startswith("[") else f"[{fixed_ids}]"
            # 解析为列表
            parent_ids = ast.literal_eval(fixed_ids)
            for parent_id in parent_ids:
                # 如果父节点尚未存在，先添加父节点（占位）
                if not G.has_node(parent_id):
                    G.add_node(parent_id, duration=None, resource=None, task_num=None)
                # 添加边：父节点 -> 当前节点
                G.add_edge(parent_id, row['collection_id'])
        except (ValueError, SyntaxError) as e:
            print(f"Error parsing start_after_collection_ids for row {index}: {row['start_after_collection_ids']}")

# 输出图的信息
print(f"Graph has {G.number_of_nodes()} nodes and {G.number_of_edges()} edges.")


subgraphs = [G.subgraph(c).copy() for c in nx.weakly_connected_components(G)]
for i, subgraph in enumerate(subgraphs):
    newlb = calc_newlb(subgraph)
    _, cplen = calc_critical_path(subgraph)
    twork = calc_twork(subgraph)

    if newlb != -1 and newlb > cplen and newlb > twork:
        print(f"----------Works for {i}----------")
        print(f"NewLB: {newlb}")
        print(f"CPLen: {cplen}")
        print(f"TWork: {twork}")
        graph1, graph2 = cut_dags(subgraph)
        print(f"Graph1: {graph1.nodes}")
        print(f"Graph2: {graph2.nodes}")

        # 可视化子图
        plt.figure(figsize=(15, 15))

        # 使用 spring 布局
        pos = nx.spring_layout(subgraph)

        # 绘制图结构
        nx.draw(subgraph, pos, with_labels=True, node_size=500, font_size=12, alpha=0.7)

        # 获取节点属性作为标签
        labels = {node: f"{node}\n{attr}" for node, attr in nx.get_node_attributes(subgraph, 'duration').items()}
        for node in subgraph.nodes(data=True):
            attributes = ', '.join(f"{key}={value}" for key, value in node[1].items())
            labels[node[0]] = f"{node[0]}\n{attributes}"

        # 绘制节点标签
        nx.draw_networkx_labels(subgraph, pos, labels=labels, font_size=8)
        plt.xlim(-1.5, 1.5)  # 设置 x 轴范围
        plt.ylim(-1.5, 1.5)  # 设置 y 轴范围

        plt.suptitle(f"Subgraph {i}", fontsize=16)
        plt.show()




# 输出图的信息
# print(f"Graph has {G.number_of_nodes()} nodes and {G.number_of_edges()} edges.")
# subgraphs = [G.subgraph(c).copy() for c in nx.weakly_connected_components(G)]
# for i, subgraph in enumerate(subgraphs):
#     print(f"Subgraph {i}:")
#     print(f"  Nodes: {list(subgraph.nodes)}")
#     print(f"  Edges: {list(subgraph.edges)}")


# plt.figure(figsize=(8, 8))
# nx.draw(subgraphs[9], with_labels=True, node_size=500, font_size=10, alpha=0.7)
# plt.title(f"Subgraph {i}")
# plt.show()
#
#
# print(calc_newlb(subgraphs[9]))
# print(calc_critical_path(subgraphs[9]))
# print(calc_twork(subgraphs[9]))
#
# print(calc_newlb(subgraphs[11793]))
# print(calc_critical_path(subgraphs[11793]))
# print(calc_twork(subgraphs[11793]))
# plt.figure(figsize=(8, 8))
# nx.draw(subgraphs[11793], with_labels=True, node_size=500, font_size=10, alpha=0.7)
# plt.title(f"Subgraph {i}")
# plt.show()
# print("Nodes and their attributes:")
# for node, attributes in subgraphs[9].nodes(data=True):  # 获取节点及其属性
#     print(f"Node: {node}, Attributes: {attributes}")
#
# # 打印图的节点总数
# print(f"\nTotal number of nodes: {G.number_of_nodes()}")
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

# Traverse all nodes
for source in G.nodes:
    # Check single node path (self-loop case)
    current_duration = G.nodes[source]['duration']
    if current_duration > max_duration:
        max_duration = current_duration
        longest_path = [source]

    # Check paths between distinct source and target nodes
    for target in G.nodes:
        if source != target and nx.has_path(G, source, target):
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
print(G.nodes[377806921258]['duration'])

# Graph Visualization
# pos = nx.spring_layout(G)
# plt.figure(figsize=(12, 12))
# nx.draw(G, pos, with_labels=True, node_color='lightblue', node_size=500, font_size=4, alpha=0.7)
# plt.title('Graph Visualization')
# plt.show()




# 假设df是从CSV读取的包含collection_id, time等信息的DataFrame

# 定义计算CPLen（Critical Path Length）的函数
def calculate_cplen(graph, path):
    cplen = 0
    for node in path:
        cplen += graph.nodes[node]['duration']
    return cplen


# 定义计算TWork的函数
def calculate_twork(graph, path):
    TWork_CPU = 0
    TWork_MEM = 0
    for node in path:
        TWork_CPU += graph.nodes[node]['cpu_usage'] * graph.nodes[node]['duration']
        TWork_MEM += graph.nodes[node]['memory_usage'] * graph.nodes[node]['duration']
    return max(TWork_CPU, TWork_MEM)

# 获取所有从起始节点到终止节点的路径
# all_paths = list(nx.all_simple_paths(G, source='start_node', target='end_node'))



# 定义计算 ModCP 的函数
def calculate_modcp(graph, path):
    modcp = 0
    stage_cplen_twork_max = []  # 用于存储每个stage的max(CPLen, TWork)
    min_durations = []  # 用于存储每个阶段的最小任务持续时间

    # 计算每个阶段的最大 CPLen 或 TWork
    for node in path:
        cplen = calculate_cplen(graph, [node])
        twork = calculate_twork(graph, [node])
        stage_cplen_twork_max.append(max(cplen, twork))

    # 计算每个阶段的最小任务持续时间
    for node in path:
        duration = graph.nodes[node]['duration']
        min_durations.append(duration)

    # 取当前阶段的max(CPLen, TWork) 加上其他阶段的最小持续时间
    for i, stage_max in enumerate(stage_cplen_twork_max):
        modcp = max(modcp, stage_max + sum(min_durations[:i] + min_durations[i + 1:]))

    return modcp


# 计算所有路径的 ModCP 值
# 计算所有路径的 ModCP 值，包括单一节点的情况

path_modcp_values = []
for source in G.nodes:
    for target in G.nodes:
        # 处理单一节点的情况，即 source == target
        if source == target:
            modcp_value = calculate_modcp(G, [source])  # 单节点路径
            path_modcp_values.append(([source], modcp_value))
        # 处理 source != target 的情况
        elif nx.has_path(G, source, target):
            for path in nx.all_simple_paths(G, source=source, target=target):
                modcp_value = calculate_modcp(G, path)
                path_modcp_values.append((path, modcp_value))


# 输出结果
# for path, modcp_value in path_modcp_values:
#     print(f"Path: {path}, ModCP: {modcp_value}")

partitions = [list(G.nodes)]  # 这里假设整个图作为一个单一的分区。如果有多个子图，你可以替换此列表

# 计算 NewLB
newlb = 0
for partition in partitions:
    cplen = calculate_cplen(G, partition)
    twork = calculate_twork(G, partition)
    # modcp = calculate_modcp(G, partition)
    # print(modcp)
    modcp = max(value for path, value in path_modcp_values)
    print(modcp)

    # 取 CPLen, TWork, ModCP 的最大值
    max_value = max(cplen, twork, modcp)
    # newlb += max_value

print(f"NewLB: {max_value}")
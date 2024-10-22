import pandas as pd
import numpy as np
from tqdm import tqdm
from google.cloud import bigquery
# Provide credentials to the runtime
from google.oauth2 import service_account
import networkx as nx
from matplotlib import pyplot as plt
import traceback
# from google.cloud.bigquery import magics

# Path to your service account key file
service_account_file = '../credentials.json'

# Create credentials object
credentials = service_account.Credentials.from_service_account_file(service_account_file)

# Initialize the BigQuery client with the credentials
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

sql_query = ('''
SELECT
        time,
        collection_id,
        type,
        user,
        parent_collection_id,
        start_after_collection_ids,
        ROW_NUMBER() OVER (PARTITION BY user ORDER BY time) as user_row_num
FROM `google.com:google-cluster-data`.clusterdata_2019_a.collection_events
WHERE collection_id NOT IN (
    SELECT collection_id
    FROM `google.com:google-cluster-data`.clusterdata_2019_a.collection_events
    WHERE time = 0
);
''')

# Convert the query result to a DataFrame
df = client.query(sql_query).to_dataframe()

unique_collection_ids = df['collection_id'].dropna().unique()  # Drop any None values and get unique values
collection_ids_array = np.array(unique_collection_ids)  # Convert to NumPy array

# Convert NumPy array to a tuple of values for SQL IN clause
collection_ids_tuple = tuple(collection_ids_array)

sql_usage = f'''SELECT
                start_time,
                end_time,
                collection_id,
                machine_id,
                collection_type,
                average_usage.cpus AS cpu_usage,
                average_usage.memory AS memory_usage,
                assigned_memory
                FROM `google.com:google-cluster-data`.clusterdata_2019_a.instance_usage
                WHERE collection_id IN {collection_ids_tuple}
                '''

df_usage = client.query(sql_usage).to_dataframe()

grouped_usage = df_usage.groupby('collection_id').agg({
    'cpu_usage': 'mean',
    'memory_usage': 'mean'
}).reset_index()\

# Use Pandas groupby to classify the tasks by user
grouped_users = df.groupby('user')

# Create a dictionary to hold subtables for each user
subtables = {user: group for user, group in grouped_users}

# Function to group related tasks based on parent_collection_id and start_after_collection_ids
def group_related_tasks(df):
    grouped_tasks = []  # List to store grouped tasks
    visited = set()  # Set to track visited tasks by their collection IDs

    # Function to find all related tasks for a given collection ID
    def find_related_tasks(collection_id, df):
        related_tasks = set()  # Set to store related collection IDs
        stack = [collection_id]  # Use stack for depth-first search

        while stack:
            current_collection_id = stack.pop()
            # print('currid',current_collection_id)
            if current_collection_id not in visited:
                visited.add(current_collection_id)
                related_tasks.add(current_collection_id)

                # Find tasks where current_collection_id is either the parent_collection_id or in start_after_collection_ids
                parent_related = df[df['parent_collection_id'] == current_collection_id]['collection_id'].tolist()

                # Handle case where start_after_collection_ids is not empty or None
                start_after_related = df[df['start_after_collection_ids'].apply(lambda ids: isinstance(ids, list) and current_collection_id in ids)]['collection_id'].tolist()

                # Add related tasks to the stack for further exploration, if they haven't been visited yet
                for task in parent_related + start_after_related:
                    if task not in visited:
                        stack.append(task)

                # Also, find the parent and start_after of the current task and process them
                parent_task = df[df['collection_id'] == current_collection_id]['parent_collection_id'].tolist()
                start_after_task = df[df['collection_id'] == current_collection_id]['start_after_collection_ids'].tolist()

                # print('par',parent_task)
                # print('start',start_after_task)
                # Modify this condition to handle empty lists of lists
                if isinstance(start_after_task, list) and len(start_after_task) > 0:  # Check if it's a valid list with non-empty elements
                  for task_list in start_after_task:
                      if isinstance(task_list, list) and len(task_list) > 0:  # Only process non-empty lists
                          for task in task_list:
                              if task not in visited:
                                  stack.append(task)

                if not pd.isna(parent_task).all():  # Check if parent_task is not NaN
                    for task in parent_task:
                        if task not in visited:
                            stack.append(task)

        return related_tasks

    # Iterate through all tasks and group them by relationships
    for collection_id in df['collection_id']:
        if collection_id not in visited:
            # Find all related tasks for the current collection_id
            related_tasks = find_related_tasks(collection_id, df)
            # Create a DataFrame for the related tasks and append to grouped_tasks
            grouped_tasks.append(df[df['collection_id'].isin(related_tasks)])

    return grouped_tasks

# Create a dictionary to store the grouped tasks for each user
grouped_subtables = {}

# Assuming subtables is a dictionary where each user has a corresponding DataFrame
for user, subtable in tqdm(subtables.items(), desc="Processing users"):
    grouped_subtables[user] = group_related_tasks(subtable)


#############################################################################
#############################################################################

############### Obtain Time, Cpu/Memory_usage_Time, Twork for each Stage (Colletion_id) ############333 

# Dictionary to store the time for each collection_id
collection_id_time = {}

# Loop through each dataframe in grouped_subtables
for user, grouped_tasks in grouped_subtables.items():
    for group, df in enumerate(grouped_tasks, 1):
        # For each unique collection_id, calculate the time (max timestamp - min timestamp)
        for collection_id in df['collection_id'].unique():
            # Get all timestamps for the given collection_id
            timestamps = df[df['collection_id'] == collection_id]['time']
            # Calculate the time for this collection_id (max timestamp - min timestamp)
            time_duration = timestamps.max() - timestamps.min()
            collection_id_time[collection_id] = time_duration

collection_id_time = {key: value / 1e6 for key, value in collection_id_time.items()}

# Get cpu_usage_time and memory_usage_time from the dictionary
result = []

for index, row in grouped_usage.iterrows():
    cpu_usage_time = row['cpu_usage'] * collection_id_time.get(row['collection_id'], 0)
    memory_usage_time = row['memory_usage'] * collection_id_time.get(row['collection_id'], 0)

    result.append([collection_id, cpu_usage_time, memory_usage_time])

# Convert result to a dictionary for efficient access
result_dict = {item[0]: {'cpu_usage_time': item[1], 'memory_usage_time': item[2]} for item in result}

# Initialize the tworks dictionary
tworks = {}

# Loop through each collection_id in result_dict
for collection_id, usage_data in result_dict.items():
    # Select the larger one between cpu_usage_time and memory_usage_time for this collection_id
    tworks[collection_id] = max(usage_data.get('cpu_usage_time', 0), usage_data.get('memory_usage_time', 0))

###############################################################
###############################################################

######################## Function to make graph, calculate CPLength, Twork, ModCP #######################

def build_user_graph(grouped_tasks):
    """
    Function to build a directed graph G for each user based on the grouped tasks.
    
    Parameters:
    grouped_tasks (list): A list of grouped tasks DataFrame for a user.

    Returns:
    G (networkx.DiGraph): A directed graph G built for the user.
    """
    G = nx.DiGraph()

    # Iterate over each group of tasks (grouped DataFrames) for the user
    for group, df in enumerate(grouped_tasks, 1):
        # Find unique collection_ids to group rows into nodes
        if len(df) > 1:
            for collection_id in df['collection_id'].unique():
                # Create the node for each collection_id
                G.add_node(int(collection_id), group=group)

                # Filter rows corresponding to the collection_id
                sub_df = df[df['collection_id'] == collection_id]

                # Add edges based on parent_collection_id
                for _, row in sub_df.iterrows():
                    parent = row['parent_collection_id']
                    if pd.notna(parent) and isinstance(parent, (int, np.integer)):  # Ensuring parent is a valid integer
                        G.add_edge(int(collection_id), int(parent))

                    # Add edges based on start_after_collection_ids
                    if isinstance(row['start_after_collection_ids'], (list, np.ndarray, pd.Series)) and len(row['start_after_collection_ids']) > 0:
                        for start_after in row['start_after_collection_ids']:
                            if isinstance(start_after, (int, np.integer)):  # Ensuring start_after is a valid integer
                                G.add_edge(int(collection_id), int(start_after))

    return G

def calculate_dag_max_times(components, collection_id_time, G):
    """
    This function calculates the maximum path time for each weakly connected component (DAG)
    in the graph components.

    Parameters:
    components (list): List of weakly connected components (subgraphs).
    collection_id_time (dict): A dictionary where keys are node IDs and values are their times.
    G (networkx.DiGraph): The original graph from which components are derived.

    Returns:
    dict: A dictionary with maximum path times for each component (DAG).
    """
    # Initialize a dictionary to store the maximum path time for each DAG
    dag_max_time_dict = {}

    # Loop through each component
    for component_index, component in enumerate(components):
        # Create a subgraph for this component
        subgraph = G.subgraph(component).copy()

        # Find all source and target nodes (start and end points for paths)
        sources = [n for n in subgraph.nodes if subgraph.in_degree(n) == 0]
        targets = [n for n in subgraph.nodes if subgraph.out_degree(n) == 0]

        # Step 2.1: Iterate over all possible paths from source to target nodes
        path_times = []
        for source in sources:
            for target in targets:
                # Find all paths between the source and target
                for path in nx.all_simple_paths(subgraph, source=source, target=target):
                    # Step 2.2: Sum up the time for all collection_ids on this path
                    path_time = sum(collection_id_time.get(node, 0) for node in path)
                    path_times.append(path_time)

        # Step 2.3: Find the maximum path time for this component
        max_path_time = max(path_times) if path_times else 0

        # Store the maximum path time for this component (DAG)
        dag_max_time_dict[component_index] = max_path_time

    # Return the dictionary with maximum path times per DAG
    return dag_max_time_dict

def calculate_usage_times(components, result_dict, G):
    """
    This function calculates the total CPU and memory usage time for each weakly connected component (DAG)
    in the graph and computes the twork as the maximum of CPU and memory usage times for each component.

    Parameters:
    components (list): List of weakly connected components (subgraphs).
    result_dict (dict): A dictionary where each key is a node ID and the values are dictionaries containing
                        'cpu_usage_time' and 'memory_usage_time'.
    G (networkx.DiGraph): The original graph from which components are derived.

    Returns:
    tuple: Three numpy arrays:
           - cpu_usage_time_per_dag: Total CPU usage time for each DAG.
           - memory_usage_time_per_dag: Total memory usage time for each DAG.
           - twork: Maximum of CPU and memory usage times for each DAG.
    """
    # Initialize lists to store results
    cpu_usage_time_per_dag = []
    memory_usage_time_per_dag = []
    twork = []

    # Loop through each component (DAG)
    for component in components:
        # Create a subgraph for this component
        subgraph = G.subgraph(component).copy()

        # Initialize sums for this DAG
        total_cpu_usage_time = 0
        total_memory_usage_time = 0

        # Get all collection_ids in the DAG
        for node in subgraph.nodes:
            # Get the cpu_usage_time and memory_usage_time from result_dict
            cpu_usage_time = result_dict.get(node, {}).get('cpu_usage_time', 0)
            memory_usage_time = result_dict.get(node, {}).get('memory_usage_time', 0)

            # Add to the total CPU and memory usage time for this DAG
            total_cpu_usage_time += cpu_usage_time
            total_memory_usage_time += memory_usage_time

        # Append results for this DAG
        cpu_usage_time_per_dag.append(total_cpu_usage_time)
        memory_usage_time_per_dag.append(total_memory_usage_time)

        # Calculate twork as the maximum of total CPU usage time and total memory usage time for this DAG
        twork.append(max(total_cpu_usage_time, total_memory_usage_time))

    # Convert to numpy arrays
    cpu_usage_time_per_dag = np.array(cpu_usage_time_per_dag)
    memory_usage_time_per_dag = np.array(memory_usage_time_per_dag)
    twork = np.array(twork)

    # Return the results
    return cpu_usage_time_per_dag, memory_usage_time_per_dag, twork

def calculate_modcp(components, collection_id_time, tworks, G):
    """
    This function calculates the modcp (modified critical path value) for each weakly connected component (DAG)
    in the graph and returns a dictionary with modcp values for each component.

    Parameters:
    components (list): List of weakly connected components (subgraphs).
    collection_id_time (dict): A dictionary where keys are node IDs and values are times for collection IDs.
    tworks (dict): A dictionary where keys are node IDs and values are their work times.
    G (networkx.DiGraph): The original graph from which components are derived.

    Returns:
    dict: A dictionary with modcp values for each DAG.
    """
    # Initialize a dictionary to store the modcp for each DAG
    modcp_dict = {}

    # Loop through each component
    for component_index, component in enumerate(components):
        # Create a subgraph for this component
        subgraph = G.subgraph(component).copy()

        # Find all source and target nodes (start and end points for paths)
        sources = [n for n in subgraph.nodes if subgraph.in_degree(n) == 0]
        targets = [n for n in subgraph.nodes if subgraph.out_degree(n) == 0]

        # Initialize to store the largest path value for this DAG
        largest_path_value = 0

        # Loop through each source-target pair and find all paths
        for source in sources:
            for target in targets:
                # Find all paths between the source and target
                all_paths = list(nx.all_simple_paths(subgraph, source=source, target=target))

                # Loop through each path and calculate the modcp
                for path in all_paths:
                    # Initialize a list to store values for each stage in the path
                    stage_values = []

                    # Loop through the stages (collection_ids) in the path
                    for i, collection_id in enumerate(path):
                        # Pick the larger value between collection_id_time and tworks for this stage
                        larger_value = max(collection_id_time.get(collection_id, 0), tworks.get(collection_id, 0))

                        # For the other stages in the path, sum up their time (from collection_id_time)
                        other_stage_times = sum(
                            collection_id_time.get(other_collection_id, 0)
                            for other_collection_id in path if other_collection_id != collection_id
                        )

                        # Calculate the total value for this stage (larger_value + sum of other stage times)
                        stage_values.append(larger_value + other_stage_times)

                    # Find the stage with the largest value in this path
                    largest_stage_value_in_path = max(stage_values)

                    # Update the largest path value for the DAG if this path's stage has the largest value
                    largest_path_value = max(largest_path_value, largest_stage_value_in_path)

        # Store the largest path value for this DAG as modcp, using a unique identifier (component_index)
        modcp_dict[component_index] = largest_path_value

    # Return the modcp dictionary
    return modcp_dict

def newlb(components, collection_id_time, result_dict, tworks, G, user):
    # Step 1: Calculate each function
    dag_max_time_dict = calculate_dag_max_times(components, collection_id_time, G)
    cpu_usage_time_per_dag, memory_usage_time_per_dag, twork = calculate_usage_times(components, result_dict, G)
    modcp_dict = calculate_modcp(components, collection_id_time, tworks, G)

        # Find the maximizer among these three dictionaries
    newlb_dict = {}
    for component_index in range(len(components)):
        dag_max_time = dag_max_time_dict.get(component_index, 0)
        modcp = modcp_dict.get(component_index, 0)
        twork_value = twork[component_index]
        maximized_value = max(dag_max_time, modcp, twork_value)
        newlb_dict[component_index] = maximized_value

    # Plot and save the histogram for this user
    plt.hist(list(newlb_dict.values()), bins=100, color='blue', edgecolor='black')
    plt.title(f'Histogram of Newlb for User {user}')
    plt.xlabel('Maximized Values')
    plt.ylabel('Frequency')

    # Save the plot as a .png file
    plt.savefig(f'histogram_newlb_user_{user}.png')
    plt.close()  # Close the plot to avoid overlapping plots for different users

    # Save the Newlb dictionary to a .txt file
    with open(f'newlb_user.txt', 'a') as f:
        f.write(f'Newlb Dictionary for User {user}:\n')
        for key, value in newlb_dict.items():
            f.write(f'{key}: {value}\n')

    return newlb_dict

#################################################################
#################################################################

######################### Main Area #############################
# Loop through each user and calculate separately
user_newlb_dicts = {}
for user, grouped_tasks in grouped_subtables.items():
    # Step 1: Build the graph for this user
    G = build_user_graph(grouped_tasks)

    # Step 2: Calculate Newlb and plot for this user
    components = list(nx.weakly_connected_components(G))
    newlb_dict = newlb(components, collection_id_time, result_dict, tworks, G, user)
    user_newlb_dicts[user] = newlb_dict

    print(f"Newlb Dictionary for User {user} saved in newlb_user.txt")



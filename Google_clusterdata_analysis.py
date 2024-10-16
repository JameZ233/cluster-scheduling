import pandas as pd
import numpy as np
from tqdm import tqdm
import altair as alt
from google.cloud import bigquery
# Provide credentials to the runtime
from google.oauth2 import service_account
# from google.cloud.bigquery import magics

# Path to your service account key file
service_account_file = '../credentials.json'

# Create credentials object
credentials = service_account.Credentials.from_service_account_file(service_account_file)

# Initialize the BigQuery client with the credentials
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

#Loading Data for collection events
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
    LIMIT 10000
''')

# Convert the query result to a DataFrame
df = client.query(sql_query).to_dataframe()
df['time'] = pd.to_numeric(df['time'], errors='coerce').astype(float)
df['collection_id'] = pd.to_numeric(df['collection_id'], errors='coerce').astype(float)
df['parent_collection_id'] = pd.to_numeric(df['parent_collection_id'], errors='coerce').astype(float)
df['start_after_collection_ids'] = pd.to_numeric(df['start_after_collection_ids'], errors='coerce').astype(float)

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


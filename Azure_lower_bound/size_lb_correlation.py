import json
import matplotlib.pyplot as plt
import numpy as np

def load_json(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)

results = load_json("results.json")
task_counts = load_json("task_counts.json")

correlation_data = []

for entry in results:
    dag_id = entry['DAG']
    lower_bound = entry['LowerBound']

    # Exclude entries where LowerBound is 3600
    if lower_bound == 3600:
        continue

    # Get task count for the DAG ID
    if dag_id in task_counts:
        task_count = task_counts[dag_id]
        correlation_data.append((task_count, lower_bound))

task_counts_list, lower_bounds_list = zip(*correlation_data)

clipped_task_counts = np.clip(task_counts_list, None, 5000)  # Cap task counts at 5000
clipped_lower_bounds = np.clip(lower_bounds_list, None, 1500)  # Cap lower bounds at 1500

# Plot the correlation
plt.scatter(clipped_task_counts, clipped_lower_bounds, alpha=0.7)
plt.title("Correlation Between DAG Size and LowerBound (Scaled)")
plt.xlabel("DAG Size (Number of Tasks, clipped at 5000)")
plt.ylabel("LowerBound (clipped at 1500)")
plt.grid(True)
plt.savefig("correlation_plot_scaled.png")  # Save the plot locally
plt.show()

with open("correlation_data.json", "w") as f:
    json.dump(correlation_data, f, indent=4)


for task_count, lower_bound in correlation_data:
    print(f"Task Count: {task_count}, LowerBound: {lower_bound}")

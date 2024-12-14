import json
import matplotlib.pyplot as plt
from collections import Counter
import numpy as np

# Load JSON data from file
with open("task_counts.json", "r") as f:
    task_counts = json.load(f)  # task_counts is already a dictionary

# Count the frequencies of task sizes
size_frequency = Counter(task_counts.values())

# Prepare data for histogram
sizes = list(size_frequency.keys())
frequencies = list(size_frequency.values())

# Use a log scale for x-axis to handle large task counts
plt.figure(figsize=(10, 6))
plt.bar(sizes, frequencies, width=0.5, edgecolor='black', alpha=0.7)
plt.xscale('log')  # Apply logarithmic scale to x-axis
plt.title("Task Size Distribution Across Tenants (Logarithmic Scale)")
plt.xlabel("Number of Tasks (Log Scale)")
plt.ylabel("Frequency")
plt.grid(axis='y', linestyle='--', alpha=0.7)

# Save the plot as an image file
plt.savefig("task_size_distribution_log.png")
plt.show()

# Save the size frequency to a JSON file for additional analysis if needed
with open("size_frequency.json", "w") as f:
    json.dump(size_frequency, f, indent=4)

print("Histogram saved as 'task_size_distribution_log.png'")
print("Size frequency saved as 'size_frequency.json'")

import json
import matplotlib.pyplot as plt
import numpy as np

# Load JSON files
def load_json(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)

results = load_json("results.json")


lower_bounds = [entry['LowerBound'] for entry in results if entry['LowerBound'] != 3600]

# Compute the threshold under which most LowerBound values are contained
lower_bound_threshold = np.percentile(lower_bounds, 95)  # 95th percentile

print(f"The LowerBound threshold under which most values are contained: {lower_bound_threshold}")


plt.hist(lower_bounds, bins=50, alpha=0.7, edgecolor='black')
plt.axvline(lower_bound_threshold, color='red', linestyle='dashed', linewidth=1, label=f"95th Percentile: {lower_bound_threshold}")
plt.title("Distribution of LowerBounds")
plt.xlabel("LowerBound")
plt.ylabel("Frequency")
plt.legend()
plt.grid(True)
plt.savefig("lowerbound_distribution.png")  # Save the plot locally
plt.show()

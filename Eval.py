import os
import json
from datetime import datetime

from evaluation import evaluate_methods

methods = ["SAITS", "BRITS", "CSDI"]
timespans = [25]
repeats = 1
results = evaluate_methods(methods=methods, sensorid="01975", timespans=timespans, repeats=repeats)

results_dir = "./.data/results"
os.makedirs(results_dir, exist_ok=True)

# Prepare filename
methods_str = "_".join(methods)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
filename = f"{timestamp}_{methods_str}_results.json"
filepath = os.path.join(results_dir, filename)

for timespan in timespans:
  method_results = results[timespan]
  for result in method_results:
    print(f"{result['method']} -- {timespan} years of data lead to a root mean square error: {result['average_rmse']:.4f}, and a mean absolute error: {result['average_mae']:.4f}")

# Save results as JSON
with open(filepath, "w") as f:
  json.dump(results, f, indent=2)
print(f"Results saved to {filepath}") 
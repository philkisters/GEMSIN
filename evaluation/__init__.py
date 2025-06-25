import sys
import os

import pandas as pd
import numpy as np

from .preparation import load_dwd_sensor_data
from .eval_saits import run_saits
from .eval_brits import run_brits
from .eval_csdi import run_csdi

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from data_preprocessing.dwd import preprocess_dwd

def evaluate_methods(methods: list, sensorid: str, timespans: list, repeats: int = 1):
    df = load_dwd_sensor_data(sensorid=sensorid)

    # Get the latest timestamp in the data
    latest_date = pd.to_datetime(df['timestamp']).max()

    results = {timespan: [] for timespan in timespans}
    
    for timespan in timespans:
        print(f"Evaluating over a timespan with {timespan} years.")
        offset = latest_date - pd.DateOffset(years=timespan)

        # Filter the DataFrame
        timespan_df = df[pd.to_datetime(df['timestamp']) >= offset]
        print(f"Data points from the last {timespan} years: {len(timespan_df)}")

        method_results = {method: [] for method in methods}

        for i in range(repeats):
            # Preprocess for model input
            dataset = preprocess_dwd(timespan_df)

            dataset_for_training = {
                "X": dataset['train_X'],
            }

            dataset_for_validating = {
                "X": dataset['val_X'],
                "X_ori": dataset['val_X_ori'],
            }

            dataset_for_testing = {
                "X": dataset['test_X'],
                "X_ori": dataset['test_X_ori'],
            }
            
            # TODO: going over each method once this makes sure that the data sets are identical and a real comparison is happening.
            for method in methods:
                if method == "SAITS":
                    mae, rmse = run_saits(n_steps=dataset['n_steps'], n_features=dataset['n_features'], dataset_for_training=dataset_for_training, dataset_for_validating=dataset_for_validating, dataset_for_testing=dataset_for_testing)
                
                    method_results[method].append({
                        "mae": float(mae),
                        "rmse": float(rmse)
                    })
                elif method == "BRITS":
                    mae, rmse = run_brits(n_steps=dataset['n_steps'], n_features=dataset['n_features'], dataset_for_training=dataset_for_training, dataset_for_validating=dataset_for_validating, dataset_for_testing=dataset_for_testing)
                
                    method_results[method].append({
                        "mae": float(mae),
                        "rmse": float(rmse)
                    })
                elif method == "CSDI":
                    mae, rmse = run_csdi(n_steps=dataset['n_steps'], n_features=dataset['n_features'], dataset_for_training=dataset_for_training, dataset_for_validating=dataset_for_validating, dataset_for_testing=dataset_for_testing)
                
                    method_results[method].append({
                        "mae": float(mae),
                        "rmse": float(rmse)
                    })
                else:
                    print(f"Skipping unknown method {method}")
                
                
        for method_result in method_results:
            
            # Extrahiere nur die mae- und rmse-Werte aus den Ergebnissen
            maes = [res["mae"] for res in method_results[method_result]]
            rmses = [res["rmse"] for res in method_results[method_result]]
            
            results[timespan].append({
                "method": method_result,
                "rmse": rmses,
                "mae": maes,
                "average_rmse": float(np.mean(rmses)),
                "average_mae":  float(np.mean(maes))
            })
            

    return results
import pandas as pd
import numpy as np
from k8s_metrics import get_metrics, collect_historical_data
from sklearn.linear_model import LinearRegression

def parse_k8s_resource(value):
    if isinstance(value, (int, float)):
        return float(value)
    
    value = value.lower()
    if value.endswith('m'):
        return float(value[:-1]) / 1000
    elif value.endswith('mi'):
        return float(value[:-2])
    elif value.endswith('gi'):
        return float(value[:-2]) * 1024
    else:
        return float(value)

def predict_future_usage(historical_data, resource_type):
    X = historical_data['timestamp'].values.reshape(-1, 1)
    y = historical_data[f'{resource_type}_usage'].values
    model = LinearRegression().fit(X, y)
    future_timestamp = np.array([[historical_data['timestamp'].max() + 3600]])  # Predict 1 hour into the future
    return model.predict(future_timestamp)[0]

def optimize_resources(current_metrics, historical_data):
    for col in ['cpu_request', 'cpu_limit', 'memory_request', 'memory_limit', 'cpu_usage', 'memory_usage']:
        current_metrics[col] = current_metrics[col].apply(parse_k8s_resource)
        historical_data[col] = historical_data[col].apply(parse_k8s_resource)

    optimizations = []

    for _, row in current_metrics.iterrows():
        pod_historical_data = historical_data[
            (historical_data['pod_name'] == row['pod_name']) &
            (historical_data['container_name'] == row['container_name'])
        ]

        if not pod_historical_data.empty:
            future_cpu_usage = predict_future_usage(pod_historical_data, 'cpu')
            future_memory_usage = predict_future_usage(pod_historical_data, 'memory')

            # CPU optimization
            if row['cpu_limit'] > 0:
                cpu_usage_ratio = future_cpu_usage / row['cpu_limit']
                if cpu_usage_ratio < 0.5:
                    suggested_cpu = max(future_cpu_usage * 1.2, 0.01)  # 20% buffer, minimum 10m
                    optimizations.append({
                        'pod_name': row['pod_name'],
                        'container_name': row['container_name'],
                        'resource': 'cpu',
                        'current_request': f"{row['cpu_request']}",
                        'current_limit': f"{row['cpu_limit']}",
                        'suggested_request': f"{suggested_cpu}",
                        'suggested_limit': f"{suggested_cpu * 1.5}",
                        'reason': f"Predicted CPU usage ({future_cpu_usage:.2f}) is significantly lower than the current limit"
                    })

            # Memory optimization
            if row['memory_limit'] > 0:
                memory_usage_ratio = future_memory_usage / row['memory_limit']
                if memory_usage_ratio < 0.7:
                    suggested_memory = max(future_memory_usage * 1.2, 10)  # 20% buffer, minimum 10Mi
                    optimizations.append({
                        'pod_name': row['pod_name'],
                        'container_name': row['container_name'],
                        'resource': 'memory',
                        'current_request': f"{row['memory_request']}Mi",
                        'current_limit': f"{row['memory_limit']}Mi",
                        'suggested_request': f"{suggested_memory:.0f}Mi",
                        'suggested_limit': f"{suggested_memory * 1.5:.0f}Mi",
                        'reason': f"Predicted memory usage ({future_memory_usage:.0f}Mi) is significantly lower than the current limit"
                    })

    return pd.DataFrame(optimizations)

if __name__ == '__main__':
    current_metrics = get_metrics()
    historical_data = collect_historical_data(duration_minutes=60, interval_seconds=60)
    optimizations_df = optimize_resources(current_metrics, historical_data)
    print(optimizations_df)
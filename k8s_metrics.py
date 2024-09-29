from kubernetes import client, config
from kubernetes.client import ApiClient
import pandas as pd
import time

def get_metrics():
    config.load_kube_config()
    api = client.CoreV1Api()
    api_client = ApiClient()

    pods = api.list_pod_for_all_namespaces().items
    metrics = []

    for pod in pods:
        pod_name = pod.metadata.name
        namespace = pod.metadata.namespace

        # Get pod metrics
        pod_metrics = api_client.call_api(
            f'/apis/metrics.k8s.io/v1beta1/namespaces/{namespace}/pods/{pod_name}',
            'GET',
            auth_settings=['BearerToken'],
            response_type='object'
        )[0]

        containers = pod.spec.containers
        for container in containers:
            container_name = container.name
            resources = container.resources
            requests = resources.requests if resources.requests else {}
            limits = resources.limits if resources.limits else {}

            # Get container metrics
            container_metrics = next(
                (c for c in pod_metrics['containers'] if c['name'] == container_name),
                {}
            )

            metrics.append({
                'pod_name': pod_name,
                'namespace': namespace,
                'container_name': container_name,
                'cpu_request': requests.get('cpu', '0'),
                'cpu_limit': limits.get('cpu', '0'),
                'memory_request': requests.get('memory', '0'),
                'memory_limit': limits.get('memory', '0'),
                'cpu_usage': container_metrics.get('usage', {}).get('cpu', '0'),
                'memory_usage': container_metrics.get('usage', {}).get('memory', '0')
            })

    return pd.DataFrame(metrics)

def collect_historical_data(duration_minutes=60, interval_seconds=60):
    historical_data = []
    end_time = time.time() + (duration_minutes * 60)

    while time.time() < end_time:
        metrics = get_metrics()
        metrics['timestamp'] = time.time()
        historical_data.append(metrics)
        time.sleep(interval_seconds)

    return pd.concat(historical_data, ignore_index=True)

if __name__ == '__main__':
    df = get_metrics()
    print(df)
from k8s_metrics import get_metrics, collect_historical_data
from optimizer import optimize_resources

def main():
    print("Collecting current Kubernetes metrics...")
    current_metrics = get_metrics()
    print("Current metrics collected.")

    print("\nCollecting historical data (this will take about 60 minutes)...")
    historical_data = collect_historical_data(duration_minutes=60, interval_seconds=60)
    print("Historical data collected.")

    print("\nGenerating optimization suggestions...")
    optimizations_df = optimize_resources(current_metrics, historical_data)

    if optimizations_df.empty:
        print("No optimizations suggested.")
    else:
        print("Suggested optimizations:")
        print(optimizations_df)

        print("\nTo apply these optimizations, you would need to update your Kubernetes deployments.")
        print("For each row in the optimizations dataframe:")
        print("1. Identify the deployment for the pod")
        print("2. Update the resource requests and limits in the deployment YAML")
        print("3. Apply the updated deployment using 'kubectl apply -f updated_deployment.yaml'")

    print("\nAdditional insights:")
    print(f"Total pods analyzed: {len(current_metrics)}")
    print(f"Pods with suggested optimizations: {len(optimizations_df['pod_name'].unique())}")
    print(f"Total optimization suggestions: {len(optimizations_df)}")

    resource_savings = optimizations_df.apply(lambda row: 
        parse_k8s_resource(row['current_limit']) - parse_k8s_resource(row['suggested_limit']), axis=1)
    print(f"Potential resource savings: {resource_savings.sum():.2f} CPU cores and {resource_savings.sum():.0f}Mi memory")

if __name__ == '__main__':
    main()
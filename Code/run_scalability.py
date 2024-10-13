import resource
import subprocess
import psutil
import time

# Increase the limit of open files
soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
resource.setrlimit(resource.RLIMIT_NOFILE, (min(soft_limit * 10, hard_limit), hard_limit))

def run_tests():
    print("Running Baseline Test (No Kafka, No Celery)...")
    baseline_metrics = run_subprocess_and_measure("python3.12", "tests/test_baseline.py")
    
    print("Running Scalability Test (With Kafka and Celery)...")
    scalability_metrics = run_subprocess_and_measure("python3.12", "src/main.py")
    
    # Compare results between baseline and scalability tests
    print_metrics_comparison(baseline_metrics, scalability_metrics)

def run_subprocess_and_measure(command, script_name):
    """
    Run a subprocess and measure performance metrics like CPU, memory, and time.
    """
    start_time = time.time()
    process = subprocess.Popen([command, script_name], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Monitor resource usage during the subprocess run
    cpu_percentages = []
    memory_usages = []
    
    # Use psutil to track CPU and memory usage while the subprocess is running
    while process.poll() is None:
        cpu_percentages.append(psutil.cpu_percent(interval=1))
        memory_usages.append(psutil.virtual_memory().percent)

    process.wait()  # Ensure process finishes before measuring final time
    end_time = time.time()

    # Collect final metrics
    total_time = end_time - start_time
    avg_cpu = sum(cpu_percentages) / len(cpu_percentages) if cpu_percentages else 0
    avg_memory = sum(memory_usages) / len(memory_usages) if memory_usages else 0

    metrics = {
        'total_time': total_time,
        'avg_cpu': avg_cpu,
        'avg_memory': avg_memory
    }

    print(f"Test '{script_name}' completed in {total_time:.2f} seconds")
    print(f"Average CPU usage: {avg_cpu:.2f}%")
    print(f"Average Memory usage: {avg_memory:.2f}%")
    
    return metrics

def print_metrics_comparison(baseline_metrics, scalability_metrics):
    """
    Print the comparison between baseline and scalability metrics.
    """
    print("\n===== Metrics Comparison (Baseline vs CMAS Scalability) =====")
    print(f"Baseline Total Time: {baseline_metrics['total_time']:.2f} seconds")
    print(f"CMAS Total Time: {scalability_metrics['total_time']:.2f} seconds")
    
    print(f"Baseline CPU Usage: {baseline_metrics['avg_cpu']:.2f}%")
    print(f"CMAS CPU Usage: {scalability_metrics['avg_cpu']:.2f}%")

    print(f"Baseline Memory Usage: {baseline_metrics['avg_memory']:.2f}%")
    print(f"CMAS Memory Usage: {scalability_metrics['avg_memory']:.2f}%")

if __name__ == "__main__":
    run_tests()

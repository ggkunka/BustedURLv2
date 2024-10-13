import resource
import subprocess
import psutil
import time
import signal
import os
import multiprocessing as mp
import logging  # Import logging
from multiprocessing import Manager
from datetime import datetime  # For timestamp in log filename
from src.ensemble_model import EnsembleModel  # Import EnsembleModel
from src.utils.model_helper import load_data_incrementally  # Import necessary functions
from kafka_broker import send_message  # Import send_message from kafka_broker.py

# Create a timestamp for log filename
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Initialize the logger with both console and file logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Create the logger
logger = logging.getLogger("ScalabilityTestLogger")

# Create file handler to log to a file with a timestamped name
log_filename = f"scalability_test_{timestamp}.log"
file_handler = logging.FileHandler(log_filename)

# Set log level for file handler
file_handler.setLevel(logging.INFO)

# Create a formatter for the log file (optional, can use the same as console)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# Add file handler to the logger
logger.addHandler(file_handler)

# Increase the limit of open files
soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
resource.setrlimit(resource.RLIMIT_NOFILE, (min(soft_limit * 10, hard_limit), hard_limit))

def start_kafka():
    print("Starting Kafka...")
    kafka_process = subprocess.Popen(
        ["/home/yzhang10/kafka/bin/kafka-server-start.sh", "/home/yzhang10/kafka/config/server.properties"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    time.sleep(10)  # Wait for Kafka to initialize
    return kafka_process

def start_celery():
    print("Starting Celery worker...")
    celery_process = subprocess.Popen(
        ["celery", "-A", "celery_worker", "worker", "--loglevel=info"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    time.sleep(5)  # Wait for Celery to initialize
    return celery_process

def stop_process(process, name):
    print(f"Stopping {name}...")
    if process.poll() is None:  # If the process is still running
        process.send_signal(signal.SIGTERM)  # Send termination signal
        try:
            process.wait(timeout=10)  # Wait for process to terminate
        except subprocess.TimeoutExpired:
            process.kill()  # Force kill if it does not stop
    print(f"{name} stopped.")

def print_progress(progress_tracker):
    """
    Print progress every 30 seconds.
    """
    total_chunks = progress_tracker['total_chunks']
    while progress_tracker['processed_chunks'] < total_chunks:
        logger.info(f"Progress: {progress_tracker['processed_chunks']} chunks processed out of {total_chunks}.")
        time.sleep(30)

def process_chunk_kafka(model, chunk, results_queue, progress_tracker):
    """
    Processes a chunk using Kafka and Celery in a CMAS-like fashion.
    Each agent (worker) processes the chunk in parallel.
    """
    for _, row in chunk.iterrows():
        url = row['url']
        label = row['label']  # 1 for malicious, 0 for benign
        features = model.extract_features(url)
        prediction = model.classify(features)

        # Send prediction via Kafka
        send_message('predictions', {'url': url, 'prediction': prediction, 'label': label})

    # Update the progress tracker
    progress_tracker['processed_chunks'] += 1

def run_tests():
    kafka_process = None
    celery_process = None
    manager = Manager()

    # Shared dictionary to track progress
    progress_tracker = manager.dict()
    progress_tracker['processed_chunks'] = 0
    progress_tracker['total_chunks'] = 0

    try:
        # Start Kafka and Celery
        kafka_process = start_kafka()
        celery_process = start_celery()

        logger.info("Running Baseline Test (No Kafka, No Celery)...")
        baseline_metrics = run_subprocess_and_measure("python3.12", "tests/test_baseline.py")

        logger.info("Running Scalability Test (With Kafka and Celery)...")

        # Start the progress printer as a process
        progress_process = mp.Process(target=print_progress, args=(progress_tracker,))
        progress_process.start()

        scalability_metrics = run_scalability_test(progress_tracker)

        # Ensure the progress process is stopped
        progress_process.join()

        # Compare results between baseline and scalability tests
        print_metrics_comparison(baseline_metrics, scalability_metrics)

    finally:
        # Ensure Kafka and Celery are stopped even if an error occurs
        if kafka_process:
            stop_process(kafka_process, "Kafka")
        if celery_process:
            stop_process(celery_process, "Celery")

def run_scalability_test(progress_tracker):
    """
    This function runs the scalability test and tracks progress.
    """
    model = EnsembleModel()
    dataset_path = 'data/cleaned_data_full.csv'

    # Step 1: Run with Kafka and Celery (distributed processing)
    logger.info("Running Scalability Test (With Kafka and Celery)...")
    start_time = time.time()

    num_cpus = 24
    pool = mp.Pool(processes=num_cpus)
    results_queue = mp.Queue()

    # Set the total chunks count
    total_records = 0
    # Pre-count the total number of chunks
    progress_tracker['total_chunks'] = sum(1 for _ in load_data_incrementally(dataset_path, chunk_size=100))

    # Start CPU and memory tracking in a separate process
    cpu_percentages = []
    memory_usages = []

    def track_resources(progress_tracker, cpu_list, mem_list):
        while progress_tracker['processed_chunks'] < progress_tracker['total_chunks']:
            cpu = psutil.cpu_percent(interval=1)
            mem = psutil.virtual_memory().percent
            cpu_list.append(cpu)
            mem_list.append(mem)

    resource_manager = Manager()
    cpu_list = resource_manager.list()
    mem_list = resource_manager.list()

    resource_tracker = mp.Process(target=track_resources, args=(progress_tracker, cpu_list, mem_list))
    resource_tracker.start()

    # Process each chunk
    for chunk in load_data_incrementally(dataset_path, chunk_size=100):
        total_records += len(chunk)
        logger.info(f"Processing chunk of size {len(chunk)}.")

        # Submit the chunk to the multiprocessing pool
        pool.apply_async(process_chunk_kafka, args=(model, chunk, results_queue, progress_tracker))

    pool.close()
    pool.join()

    # Wait for resource tracking to finish
    resource_tracker.join()

    end_time = time.time()
    total_time = end_time - start_time
    avg_cpu = sum(cpu_list) / len(cpu_list) if cpu_list else 0
    avg_memory = sum(mem_list) / len(mem_list) if mem_list else 0

    logger.info(f"Total records processed with Kafka and Celery: {total_records}")
    logger.info(f"Processing time with Kafka and Celery: {total_time:.2f} seconds")
    logger.info(f"Average CPU usage: {avg_cpu:.2f}%")
    logger.info(f"Average Memory usage: {avg_memory:.2f}%")

    return {
        'total_time': total_time,
        'avg_cpu': avg_cpu,
        'avg_memory': avg_memory,
        'total_records': total_records
    }

def run_subprocess_and_measure(command, script_name):
    """
    Run a subprocess and measure performance metrics like CPU, memory, and time.
    """
    start_time = time.time()
    process = subprocess.Popen([command, script_name], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    cpu_percentages = []
    memory_usages = []

    while process.poll() is None:
        cpu_percentages.append(psutil.cpu_percent(interval=1))
        memory_usages.append(psutil.virtual_memory().percent)

    process.wait()
    end_time = time.time()

    total_time = end_time - start_time
    avg_cpu = sum(cpu_percentages) / len(cpu_percentages) if cpu_percentages else 0
    avg_memory = sum(memory_usages) / len(memory_usages) if memory_usages else 0

    logger.info(f"Test '{script_name}' completed in {total_time:.2f} seconds")
    logger.info(f"Average CPU usage: {avg_cpu:.2f}%")
    logger.info(f"Average Memory usage: {avg_memory:.2f}%")

    return {
        'total_time': total_time,
        'avg_cpu': avg_cpu,
        'avg_memory': avg_memory
    }

def print_metrics_comparison(baseline_metrics, scalability_metrics):
    """
    Print the comparison between baseline and scalability metrics.
    """
    logger.info("\n===== Metrics Comparison (Baseline vs CMAS Scalability) =====")
    
    # Print total time
    logger.info(f"Baseline Total Time: {baseline_metrics['total_time']:.2f} seconds")
    logger.info(f"CMAS Total Time: {scalability_metrics['total_time']:.2f} seconds")
    
    # Print CPU usage
    logger.info(f"Baseline Average CPU Usage: {baseline_metrics['avg_cpu']:.2f}%")
    logger.info(f"CMAS Average CPU Usage: {scalability_metrics['avg_cpu']:.2f}%")
    
    # Print Memory usage
    logger.info(f"Baseline Average Memory Usage: {baseline_metrics['avg_memory']:.2f}%")
    logger.info(f"CMAS Average Memory Usage: {scalability_metrics['avg_memory']:.2f}%")
    
    # Print total records processed
    logger.info(f"Baseline Total Records Processed: {baseline_metrics.get('total_records', 'N/A')}")
    logger.info(f"CMAS Total Records Processed: {scalability_metrics.get('total_records', 'N/A')}")

if __name__ == "__main__":
    run_tests()

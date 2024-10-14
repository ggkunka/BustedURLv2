import resource
import subprocess
import psutil
import time
import signal
import os
import multiprocessing as mp
import logging
from multiprocessing import Manager
from datetime import datetime
from src.ensemble_model import EnsembleModel
from src.utils.model_helper import load_data_incrementally
from kafka_broker import send_message
from celery import Celery
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

# Timestamp for log filename
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Initialize logger with both console and file logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ScalabilityTestLogger")
log_filename = f"scalability_test_{timestamp}.log"
file_handler = logging.FileHandler(log_filename)
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Increase the limit of open files
soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
resource.setrlimit(resource.RLIMIT_NOFILE, (min(soft_limit * 10, hard_limit), hard_limit))

def start_zookeeper():
    """Start Zookeeper process."""
    logger.info("Starting Zookeeper...")
    zookeeper_process = subprocess.Popen(
        ["/home/yzhang10/kafka/bin/zookeeper-server-start.sh", "/home/yzhang10/kafka/config/zookeeper.properties"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    time.sleep(10)  # Wait for Zookeeper to initialize
    return zookeeper_process

def start_kafka():
    """Start Kafka process."""
    logger.info("Starting Kafka...")
    kafka_process = subprocess.Popen(
        ["/home/yzhang10/kafka/bin/kafka-server-start.sh", "/home/yzhang10/kafka/config/server.properties"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    time.sleep(10)  # Wait for Kafka to initialize
    return kafka_process

def start_celery():
    """Start Celery worker."""
    logger.info("Starting Celery worker...")
    celery_process = subprocess.Popen(
        ["celery", "-A", "celery_worker", "worker", "--loglevel=info"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    time.sleep(5)  # Wait for Celery to initialize
    return celery_process

def create_kafka_topic():
    """Create the Kafka 'predictions' topic."""
    logger.info("Creating Kafka topic 'predictions'...")
    subprocess.run([
        "/home/yzhang10/kafka/bin/kafka-topics.sh", "--create", "--topic", "predictions",
        "--bootstrap-server", "localhost:9092", "--partitions", "1", "--replication-factor", "1"
    ])

def stop_process(process, name):
    """Stop a given process."""
    logger.info(f"Stopping {name}...")
    if process.poll() is None:  # If the process is still running
        process.send_signal(signal.SIGTERM)  # Send termination signal
        try:
            process.wait(timeout=10)  # Wait for process to terminate
        except subprocess.TimeoutExpired:
            process.kill()  # Force kill if it does not stop
    logger.info(f"{name} stopped.")

def print_progress(progress_tracker):
    """Print progress every 30 seconds."""
    total_chunks = progress_tracker['total_chunks']
    while progress_tracker['processed_chunks'] < total_chunks:
        logger.info(f"Progress: {progress_tracker['processed_chunks']} chunks processed out of {total_chunks}.")
        time.sleep(30)

def process_chunk_kafka(model, chunk_dict, results_queue, progress_tracker, total_records, true_labels, predicted_labels):
    """Processes a chunk using Kafka and Celery in a CMAS-like fashion."""
    for row in chunk_dict:
        url = row['url']
        label = row['label']  # 1 for malicious, 0 for benign
        features = model.extract_features(url)
        prediction = model.classify(features)

        # Log and send prediction via Kafka
        logger.info(f"Sending prediction message to Kafka for URL: {url}")
        send_message('predictions', {'url': url, 'prediction': prediction, 'label': label})

        # Track true and predicted labels
        true_labels.append(label)
        predicted_labels.append(prediction)

    progress_tracker['processed_chunks'] += 1
    total_records.value += len(chunk_dict)  # Increment the total number of records processed

def reset_system():
    """Restart Zookeeper, Kafka, Celery, and check Redis status."""
    logger.info("Resetting system: Stopping and starting Zookeeper, Kafka, and Celery.")

    # Restart Zookeeper, Kafka, and Celery
    zookeeper_process = start_zookeeper()
    kafka_process = start_kafka()
    create_kafka_topic()
    celery_process = start_celery()

    return zookeeper_process, kafka_process, celery_process

def run_tests():
    zookeeper_process = None
    kafka_process = None
    celery_process = None
    manager = Manager()

    # Shared dictionary to track progress
    progress_tracker = manager.dict()
    progress_tracker['processed_chunks'] = 0
    progress_tracker['total_chunks'] = 0

    # Shared variable to track total records processed
    total_records = manager.Value('i', 0)

    # List to store true and predicted labels
    true_labels = manager.list()
    predicted_labels = manager.list()

    try:
        # Start the first chunk of data processing
        zookeeper_process, kafka_process, celery_process = reset_system()

        # Run the Baseline Test
        logger.info("Running Baseline Test (No Kafka, No Celery)...")
        baseline_metrics = run_subprocess_and_measure("python3.12", "tests/test_baseline.py")

        # Start the progress printer as a process
        progress_process = mp.Process(target=print_progress, args=(progress_tracker,))
        progress_process.start()

        # Run Incremental Scalability Test (Processing by chunks)
        logger.info("Running Incremental Scalability Test...")
        scalability_metrics = run_incremental_scalability_test(progress_tracker, total_records, zookeeper_process, kafka_process, celery_process, true_labels, predicted_labels)

        # Ensure the progress process is stopped
        progress_process.join()

        # Print comparison between baseline and scalability results
        print_metrics_comparison(baseline_metrics, scalability_metrics)

    finally:
        # Ensure Zookeeper, Kafka, and Celery are stopped even if an error occurs
        if kafka_process:
            stop_process(kafka_process, "Kafka")
        if celery_process:
            stop_process(celery_process, "Celery")
        if zookeeper_process:
            stop_process(zookeeper_process, "Zookeeper")

def run_incremental_scalability_test(progress_tracker, total_records, zookeeper_process, kafka_process, celery_process, true_labels, predicted_labels):
    """Run the scalability test in increments and restart the system between each chunk."""
    model = EnsembleModel()
    dataset_path = 'data/cleaned_data_full.csv'

    # Determine the number of chunks
    chunk_size = 10000
    logger.info("Processing data in chunks of size %d...", chunk_size)

    total_chunks = sum(1 for _ in load_data_incrementally(dataset_path, chunk_size=chunk_size))
    progress_tracker['total_chunks'] = total_chunks
    logger.info(f"Total chunks to process: {total_chunks}")

    celery_app = Celery('tasks', broker='redis://localhost:6379/0')

    # Track CPU and memory usage across all chunks
    cpu_percentages = []
    memory_usages = []
    start_time = time.time()

    # Iterate over each chunk and reset the system after each one
    for i, chunk in enumerate(load_data_incrementally(dataset_path, chunk_size=chunk_size), start=1):
        logger.info(f"Processing chunk {i}/{total_chunks} of size {len(chunk)}.")

        # Convert DataFrame to a list of dictionaries for Celery
        chunk_dict = chunk.to_dict(orient='records')

        # Submit the chunk to the Celery worker
        celery_app.send_task('celery_worker.run_ensemble', args=[chunk_dict])

        # Track resource usage after each chunk
        cpu_percentages.append(psutil.cpu_percent())
        memory_usages.append(psutil.virtual_memory().percent)

        logger.info(f"Chunk {i} processed. Resetting system for the next chunk...")

        # Stop and reset the system before processing the next chunk
        stop_system_services(zookeeper_process, kafka_process, celery_process)
        zookeeper_process, kafka_process, celery_process = reset_system()

    # Calculate final metrics
    total_time = time.time() - start_time
    avg_cpu = sum(cpu_percentages) / len(cpu_percentages) if cpu_percentages else 0
    avg_memory = sum(memory_usages) / len(memory_usages) if memory_usages else 0

    # Calculate Accuracy, Precision, Recall, and F1 Score
    accuracy = accuracy_score(true_labels, predicted_labels)
    precision = precision_score(true_labels, predicted_labels)
    recall = recall_score(true_labels, predicted_labels)
    f1 = f1_score(true_labels, predicted_labels)

    logger.info(f"Total records processed: {total_records.value}")
    logger.info(f"Total time taken: {total_time:.2f} seconds")
    logger.info(f"Average CPU usage: {avg_cpu:.2f}%")
    logger.info(f"Average Memory usage: {avg_memory:.2f}%")
    logger.info(f"Accuracy: {accuracy:.4f}")
    logger.info(f"Precision: {precision:.4f}")
    logger.info(f"Recall: {recall:.4f}")
    logger.info(f"F1 Score: {f1:.4f}")

    return {
        'total_time': total_time,
        'avg_cpu': avg_cpu,
        'avg_memory': avg_memory,
        'total_records': total_records.value,
        'accuracy': accuracy,
        'precision': precision,
        'recall': recall,
        'f1': f1
    }

def stop_system_services(zookeeper_process, kafka_process, celery_process):
    """Stop the running Zookeeper, Kafka, and Celery services."""
    # Gracefully stop Celery, Kafka, and Zookeeper
    stop_process(celery_process, "Celery")
    stop_process(kafka_process, "Kafka")
    stop_process(zookeeper_process, "Zookeeper")

def run_subprocess_and_measure(command, script_name):
    """Run a subprocess and measure performance metrics."""
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
    """Print comparison between baseline and scalability metrics."""
    logger.info("\n===== Metrics Comparison (Baseline vs CMAS Scalability) =====")
    logger.info(f"Baseline Total Time: {baseline_metrics['total_time']:.2f} seconds")
    logger.info(f"CMAS Total Time: {scalability_metrics['total_time']:.2f} seconds")
    logger.info(f"Baseline Average CPU Usage: {baseline_metrics['avg_cpu']:.2f}%")
    logger.info(f"CMAS Average CPU Usage: {scalability_metrics['avg_cpu']:.2f}%")
    logger.info(f"Baseline Average Memory Usage: {baseline_metrics['avg_memory']:.2f}%")
    logger.info(f"CMAS Average Memory Usage: {scalability_metrics['avg_memory']:.2f}%")
    logger.info(f"Baseline Total Records Processed: {baseline_metrics.get('total_records', 'N/A')}")
    logger.info(f"CMAS Total Records Processed: {scalability_metrics['total_records']}")
    logger.info(f"Accuracy: {scalability_metrics['accuracy']:.4f}")
    logger.info(f"Precision: {scalability_metrics['precision']:.4f}")
    logger.info(f"Recall: {scalability_metrics['recall']:.4f}")
    logger.info(f"F1 Score: {scalability_metrics['f1']:.4f}")

if __name__ == "__main__":
    run_tests()

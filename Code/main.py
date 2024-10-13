import pandas as pd
import multiprocessing as mp
from src.ensemble_model import EnsembleModel
from src.utils.model_helper import load_data_incrementally
from kafka_broker import send_message
from celery_worker import run_ensemble
import time
import logging
import psutil

# Set up logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ModelHelper")

def process_chunk_kafka(model, chunk, results_queue):
    start_time = time.time()  # Start measuring latency for this chunk
    predictions = []
    
    # Process each URL in the chunk
    for _, row in chunk.iterrows():
        url = row['url']
        label = row['label']  # 1 for malicious, 0 for benign
        features = model.extract_features(url)
        prediction = model.classify(features)

        # Send prediction via Kafka
        send_message('predictions', {'url': url, 'prediction': prediction, 'label': label})
        predictions.append(prediction)

    # Measure and log latency for processing this chunk
    latency = time.time() - start_time
    logger.info(f"Chunk processed in {latency:.2f} seconds. Predictions: {len(predictions)}")
    
    results_queue.put((predictions, latency))

def run_baseline_model(chunk, model, results_queue):
    start_time = time.time()  # Start measuring latency
    predictions = []

    # Process each URL in the chunk
    for _, row in chunk.iterrows():
        url = row['url']
        label = row['label']
        features = model.extract_features(url)
        prediction = model.classify(features)
        predictions.append(prediction)

    latency = time.time() - start_time
    logger.info(f"Baseline chunk processed in {latency:.2f} seconds.")
    
    results_queue.put((predictions, latency))

def calculate_throughput(total_records, total_time):
    """
    Calculate throughput (records processed per second).
    """
    return total_records / total_time

def main():
    model = EnsembleModel()
    dataset_path = 'data/cleaned_data_full.csv'

    # Step 1: Run with Kafka and Celery (for distributed processing across multiple agents)
    logger.info("Running Scalability Test (With Kafka and Celery)...")
    start_time = time.time()

    # Create a new pool for the Kafka and Celery test
    num_cpus = min(24, mp.cpu_count())  # Dynamically calculate CPUs
    pool = mp.Pool(processes=num_cpus)
    results_queue = mp.Queue()

    total_records = 0
    total_latency = 0

    try:
        for chunk in load_data_incrementally(dataset_path, chunk_size=100):
            total_records += len(chunk)
            pool.apply_async(process_chunk_kafka, args=(model, chunk, results_queue))

        pool.close()
        pool.join()
        
        # Collect results from the queue
        while not results_queue.empty():
            _, chunk_latency = results_queue.get()
            total_latency += chunk_latency

    except Exception as e:
        logger.error(f"Error during Kafka and Celery test: {e}")
    finally:
        pool.terminate()  # Ensure all processes are cleaned up

    total_time = time.time() - start_time
    throughput = calculate_throughput(total_records, total_time)

    logger.info(f"Total records processed with Kafka and Celery: {total_records}")
    logger.info(f"Total time with Kafka and Celery: {total_time:.2f} seconds")
    logger.info(f"Average latency per chunk: {total_latency / total_records:.2f} seconds")
    logger.info(f"Throughput (records per second): {throughput:.2f}")

    # Step 2: Baseline test without Kafka/Celery (direct parallel processing)
    logger.info("Running Baseline Test (No Kafka, No Celery)...")
    start_time = time.time()

    pool = mp.Pool(processes=num_cpus)  # Create a new pool for the baseline test
    results_queue = mp.Queue()

    total_records = 0
    total_latency = 0

    try:
        for chunk in load_data_incrementally(dataset_path, chunk_size=100):
            total_records += len(chunk)
            pool.apply_async(run_baseline_model, args=(chunk, model, results_queue))

        pool.close()
        pool.join()
        
        # Collect results from the queue
        while not results_queue.empty():
            _, chunk_latency = results_queue.get()
            total_latency += chunk_latency

    except Exception as e:
        logger.error(f"Error during Baseline test: {e}")
    finally:
        pool.terminate()

    total_time = time.time() - start_time
    throughput = calculate_throughput(total_records, total_time)

    logger.info(f"Total records processed in baseline test: {total_records}")
    logger.info(f"Baseline processing time: {total_time:.2f} seconds")
    logger.info(f"Average latency per chunk: {total_latency / total_records:.2f} seconds")
    logger.info(f"Throughput (records per second): {throughput:.2f}")

if __name__ == "__main__":
    main()

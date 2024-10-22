import os
import pandas as pd
from src.ensemble_model import EnsembleModel
from ids_ips.integration import IDS_IPS_Integration
from kafka_broker import KafkaProducer
from src.utils.logger import get_logger
from hdfs import InsecureClient
import subprocess
import sys
import numpy as np  # Add this line for numpy
import logging

# Add the BustedURLv2 folder to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Initialize logger
logger = get_logger("MainLogger")

# HDFS setup
HDFS_URL = "http://localhost:9000"
HDFS_PATH = "/phishing_urls/collected_urls.txt"
LOCAL_FILE_PATH = "/tmp/collected_urls.txt"

# Define batch size
BATCH_SIZE = 100  # You can adjust this depending on your system's capabilities

def fetch_data_from_hdfs():
    """Fetch the latest data from HDFS and store it locally for model training."""
    logger.info("Fetching data from HDFS using HDFS CLI...")

    try:
        if os.path.exists(LOCAL_FILE_PATH):
            logger.info(f"Removing existing local file: {LOCAL_FILE_PATH}")
            os.remove(LOCAL_FILE_PATH)

        cmd = f"/home/yzhang10/hadoop/bin/hdfs dfs -get {HDFS_PATH} {LOCAL_FILE_PATH}"
        subprocess.run(cmd, shell=True, check=True)

        logger.info(f"Data successfully fetched from HDFS and saved to {LOCAL_FILE_PATH}")

        data = pd.read_csv(LOCAL_FILE_PATH, header=None, names=['url', 'label'], on_bad_lines='skip')
        logger.info(f"Data loaded successfully with {len(data)} rows.")
        return data

    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to fetch data from HDFS using CLI: {str(e)}")
        return None

def batch_process_data(model, X_raw, y, batch_size=100):
    """Process data in batches and train the model."""
    num_batches = len(X_raw) // batch_size + (1 if len(X_raw) % batch_size != 0 else 0)
    for batch_num in range(num_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, len(X_raw))
        X_batch_raw = X_raw[start_idx:end_idx]
        y_batch = y[start_idx:end_idx]

        # Ensure X_batch_raw is a list of strings
        X_batch = [str(url) for url in X_batch_raw]
        logging.info(f"Processing batch {batch_num + 1}/{num_batches}...")

        # Train the model on this batch
        model.train_on_batch(X_batch, y_batch)

def main():
    logger.info("Starting BustedURL system...")

    # Initialize the ensemble model
    model = EnsembleModel()

    # Fetch the real-time dataset from HDFS
    dataset = fetch_data_from_hdfs()

    if dataset is not None:
        X_raw, y = dataset['url'].values, dataset['label'].values

        # Process and train the model in batches
        batch_process_data(model, X_raw, y)

        model.save_model('models/ensemble_model.pkl')
        logger.info("Model training completed and saved.")
    
    # Initialize IDS/IPS system
    ids_ips = IDS_IPS_Integration()
    
    def process_url(url):
        """Process URL using IDS/IPS and Ensemble Model."""
        result = ids_ips.process_incoming_url(url)
        logger.info(f"URL {url} processed with result: {result}")
    
    # Start Kafka Producer (replace KafkaBroker with KafkaProducer)
    kafka_producer = KafkaProducer()

    # Example: Simulate URL processing
    test_urls = ["http://malicious-example.com", "http://benign-example.com"]
    for url in test_urls:
        process_url(url)

    logger.info("System is now running in real-time mode.")

if __name__ == "__main__":
    main()

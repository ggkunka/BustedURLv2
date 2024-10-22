import os
import pandas as pd
from sklearn.model_selection import train_test_split
from src.ensemble_model import EnsembleModel
from ids_ips.integration import IDS_IPS_Integration
from kafka_broker import KafkaProducer
from src.utils.logger import get_logger
from hdfs import InsecureClient
import subprocess
import sys
import numpy as np
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
BATCH_SIZE = 100  # Adjust based on system capabilities

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

def batch_process_data(model, X_raw, y, batch_size=BATCH_SIZE):
    """Process data in batches using stratified sampling and evaluate metrics."""
    from sklearn.model_selection import train_test_split

    # Ensure we have a balanced batch for training using stratified sampling
    X_train, _, y_train, _ = train_test_split(X_raw, y, test_size=0.3, stratify=y)
    
    num_batches = len(X_train) // batch_size + (1 if len(X_train) % batch_size != 0 else 0)
    skipped_batches = []

    # Initialize lists to store true labels and predictions for metrics calculation
    all_y_true = []
    all_y_pred = []

    for batch_num in range(num_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, len(X_train))
        X_batch_raw = X_train[start_idx:end_idx]
        y_batch = y_train[start_idx:end_idx]

        # Ensure X_batch_raw is a list of strings
        X_batch = [str(url) for url in X_batch_raw]
        logging.info(f"Processing batch {batch_num + 1}/{num_batches}...")

        # Check if the batch contains at least two classes
        if len(set(y_batch)) < 2:
            logging.warning(f"Skipping batch {batch_num + 1} due to only one class present.")
            skipped_batches.append((X_batch, y_batch))
            continue

        # Train the model on this batch
        try:
            model.train_on_batch(X_batch, y_batch)

            # Extract features and get predictions after training for metrics calculation
            features = model.extract_features(X_batch)
            y_pred = model.classify(features)
            all_y_true.extend(y_batch)
            all_y_pred.extend(y_pred)

        except ValueError as e:
            logging.error(f"Failed to train on batch {batch_num + 1}: {e}")
            continue

    # Calculate metrics after processing all batches
    if all_y_true and all_y_pred:
        # Convert lists to numpy arrays for metric calculation
        all_y_true = np.array(all_y_true)
        all_y_pred = np.array(all_y_pred)

        # Use model's calculate_metrics method to compute and log metrics
        metrics = model.calculate_metrics(all_y_true, all_y_pred, all_y_pred)  # Pass y_pred as a placeholder for probabilities
        logging.info(f"Final training metrics: {metrics}")
    else:
        logging.warning("No valid batches were processed for metric calculation.")



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

        # Save the trained model
        model.save_model('models/ensemble_model.pkl')
        logger.info("Model training completed and saved.")
    
        # Evaluate model on the full dataset
        features = model.extract_features(X_raw)
        y_pred = model.classify(features)
        metrics = model.calculate_metrics(y, y_pred, model.classify_proba(features))
        
        logger.info(f"Final Training Metrics: {metrics}")
    
    # Initialize IDS/IPS system
    ids_ips = IDS_IPS_Integration()
      
    # Start Kafka Producer (replace KafkaBroker with KafkaProducer)
    kafka_producer = KafkaProducer()

    # Example: Simulate URL processing
    test_urls = ["http://malicious-example.com", "http://benign-example.com"]
    for url in test_urls:
        process_url(url, ids_ips)

    logger.info("System is now running in real-time mode.")

def process_url(url, ids_ips):
    """Process URL using IDS/IPS and Ensemble Model."""
    result = ids_ips.process_incoming_url(url)
    logger.info(f"URL {url} processed with result: {result}")

if __name__ == "__main__":
    main()

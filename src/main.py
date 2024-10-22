import os
import pandas as pd
from src.ensemble_model import EnsembleModel
from ids_ips.integration import IDS_IPS_Integration
from kafka_broker import KafkaProducer  # Updated to KafkaProducer
from src.utils.logger import get_logger
from hdfs import InsecureClient  # HDFS client for Python
import subprocess
import sys

# Add the BustedURLv2 folder to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Initialize logger
logger = get_logger("MainLogger")

# HDFS setup
HDFS_URL = "http://localhost:9000"
HDFS_PATH = "/phishing_urls/collected_urls.txt"
LOCAL_FILE_PATH = "/tmp/collected_urls.txt"  # Temporary local file path

def fetch_data_from_hdfs():
    """Fetch the latest data from HDFS and store it locally for model training."""
    logger.info("Fetching data from HDFS using HDFS CLI...")

    try:
        # Check if the local file exists, and remove it if it does
        if os.path.exists(LOCAL_FILE_PATH):
            logger.info(f"Removing existing local file: {LOCAL_FILE_PATH}")
            os.remove(LOCAL_FILE_PATH)

        # Use HDFS CLI to copy the file from HDFS to the local file system
        cmd = f"/home/yzhang10/hadoop/bin/hdfs dfs -get {HDFS_PATH} {LOCAL_FILE_PATH}"
        subprocess.run(cmd, shell=True, check=True)

        logger.info(f"Data successfully fetched from HDFS and saved to {LOCAL_FILE_PATH}")

        # Load and preprocess data
        data = pd.read_csv(LOCAL_FILE_PATH, header=None, names=['url', 'label'], on_bad_lines='skip')
        logger.info(f"Data loaded successfully with {len(data)} rows.")
        return data

    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to fetch data from HDFS using CLI: {str(e)}")
        return None

def main():
    logger.info("Starting BustedURL system...")

    # Initialize the ensemble model
    model = EnsembleModel()

    # Fetch the real-time dataset from HDFS
    dataset = fetch_data_from_hdfs()

    # If data is fetched successfully, proceed with model training
    if dataset is not None:
        X_raw, y = dataset['url'].values, dataset['label'].values

        # Extract features from the raw URLs
        X = [model.extract_features(url) for url in X_raw]

        # Train the model incrementally with the new data
        model.train_on_batch(X, y)
        model.save_model('models/ensemble_model.pkl')
        logger.info("Model training completed and saved.")
    
    # Initialize IDS/IPS system for real-time URL classification
    ids_ips = IDS_IPS_Integration()
    
    def process_url(url):
        """Process URL using IDS/IPS and Ensemble Model."""
        result = ids_ips.process_incoming_url(url)
        logger.info(f"URL {url} processed with result: {result}")
    
    # Start Kafka Producer (replace KafkaBroker with KafkaProducer)
    kafka_producer = KafkaProducer()
    
    # Example: Simulate URL processing (replace with real-time message consumption)
    test_urls = ["http://malicious-example.com", "http://benign-example.com"]
    for url in test_urls:
        process_url(url)

    logger.info("System is now running in real-time mode.")

if __name__ == "__main__":
    main()

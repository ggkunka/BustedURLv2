import multiprocessing as mp
from src.ensemble_model import EnsembleModel
from real_time_data_ingestion import RealTimeDataIngestion
from src.metrics import RealTimeMetrics
from ids_ips.integration import process_url_with_ids
from src.utils.logger import get_logger

logger = get_logger('ScalabilityTest')

def run_real_time_test():
    """Run real-time URL detection and metrics collection."""
    model = EnsembleModel()
    metrics = RealTimeMetrics()
    ingestion = RealTimeDataIngestion()

    # Start real-time data collection in a separate process
    ingestion_process = mp.Process(target=ingestion.start_real_time_collection)
    ingestion_process.start()

    try:
        while True:
            # Pull URL from Kafka or other sources
            url = get_url_from_kafka()  # Implement URL pulling from Kafka

            # Pass through IDS/IPS
            status = process_url_with_ids(url)
            if status == "blocked":
                logger.info(f"URL {url} was blocked by IDS/IPS.")
                continue

            # Classify the URL using the ensemble model
            prediction = model.classify(url)

            # Update metrics
            true_label = fetch_label_from_db(url)  # Get true label from a source
            metrics.update_metrics(true_label, prediction)

            # Print real-time metrics
            accuracy, precision, recall, f1 = metrics.calculate_metrics()
            logger.info(f"Real-time Metrics: Accuracy={accuracy}, Precision={precision}, Recall={recall}, F1={f1}")

    finally:
        ingestion_process.terminate()

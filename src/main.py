import os
from src.ensemble_model import EnsembleModel
from src.cmas_agents import DataCollectionAgent
from ids_ips.integration import IDSIPS
from kafka_broker import KafkaBroker
from src.utils.logger import get_logger
from real_time_data_ingestion import start_real_time_ingestion

logger = get_logger("MainLogger")

def main():
    logger.info("Starting BustedURL system...")

    # Initialize the ensemble model
    model = EnsembleModel()

    # Start real-time data ingestion
    real_time_data_agent = DataCollectionAgent()
    start_real_time_ingestion(real_time_data_agent)

    # Start IDS/IPS system
    ids_ips = IDSIPS(threshold=0.85)
    
    # Kafka Broker
    kafka = KafkaBroker()
    
    logger.info("System is now running in real-time mode.")

if __name__ == "__main__":
    main()

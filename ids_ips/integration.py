import csv
import logging
from kafka_broker import send_message
from src.ensemble_model import EnsembleModel
from src.utils.logger import get_logger
import tracemalloc  # For memory profiling

logger = get_logger("IDS_IPS")

class IDS_IPS_Integration:
    def __init__(self):
        # Load the signature database if available
        self.signature_db = self.load_signature_db()
        
        # Initialize the ensemble model and load the trained model from disk
        self.ensemble_model = EnsembleModel()
        try:
            self.ensemble_model.load_model('models/ensemble_model.pkl')
            logger.info("Successfully loaded the ensemble model for IDS/IPS integration.")
        except FileNotFoundError:
            logger.error("Trained model not found. Please ensure the model is trained and saved before use.")
            self.ensemble_model = None  # Set model to None if loading fails

    def load_signature_db(self):
        """Load the signature database from CSV."""
        try:
            with open('ids_ips/signature_db.csv') as file:
                reader = csv.DictReader(file)
                logger.info("Signature database loaded successfully.")
                return [row for row in reader]
        except FileNotFoundError:
            logger.error("Signature database not found.")
            return []

    def check_signatures(self, url):
        """Check if the URL matches any known phishing signatures."""
        for signature in self.signature_db:
            if signature['url'] in url:
                return True  # URL is a known phishing URL
        return False

    def analyze_url(self, url):
        """Analyze the URL using the Ensemble model and track memory usage."""
        if self.ensemble_model is None:
            logger.error("Ensemble model is not available for URL analysis.")
            return None

        # Memory profiling before analysis
        tracemalloc.start()
        start_snapshot = tracemalloc.take_snapshot()

        # Extract features using the ensemble model and classify the URL
        features = self.ensemble_model.extract_features([url])
        prediction = self.ensemble_model.classify(features)

        # Memory profiling after analysis
        end_snapshot = tracemalloc.take_snapshot()
        memory_diff = end_snapshot.compare_to(start_snapshot, 'lineno')
        for stat in memory_diff[:10]:
            logger.info(f"Memory usage during URL analysis: {stat}")

        return prediction

    def process_incoming_url(self, url):
        """Process a URL through the IDS/IPS system."""
        if self.check_signatures(url):
            send_message('alerts', {'url': url, 'action': 'block'})
            logger.info(f"URL {url} matches known phishing signatures. Blocking...")
            return "blocked"
        else:
            # Analyze the URL with the ensemble model for phishing detection
            prediction = self.analyze_url(url)
            if prediction is None:
                logger.error(f"Failed to analyze URL {url}. No action taken.")
                return "error"

            if prediction == 1:  # Malicious URL
                send_message('alerts', {'url': url, 'action': 'block'})
                logger.info(f"URL {url} classified as malicious by the model. Blocking...")
                return "blocked"
            else:
                logger.info(f"URL {url} classified as benign by the model. Allowing...")
                return "allowed"

# Function to process URLs in real-time
def process_url_with_ids(url):
    ids_ips = IDS_IPS_Integration()
    return ids_ips.process_incoming_url(url)

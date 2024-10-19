import csv
from kafka_broker import send_message
from src.ensemble_model import EnsembleModel

class IDS_IPS_Integration:
    def __init__(self):
        self.signature_db = self.load_signature_db()

    def load_signature_db(self):
        """Load the signature database from CSV."""
        with open('ids_ips/signature_db.csv') as file:
            reader = csv.DictReader(file)
            return [row for row in reader]

    def check_signatures(self, url):
        """Check if the URL matches any known phishing signatures."""
        for signature in self.signature_db:
            if signature['url'] in url:
                return True  # URL is a known phishing URL
        return False

    def process_incoming_url(self, url):
        """Process a URL through the IDS/IPS system."""
        if self.check_signatures(url):
            send_message('alerts', {'url': url, 'action': 'block'})
            return "blocked"
        else:
            return "allowed"

# Use this in real-time
def process_url_with_ids(url):
    ids_ips = IDS_IPS_Integration()
    return ids_ips.process_incoming_url(url)

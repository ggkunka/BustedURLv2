#real_time_data_ingestion.py
import time
import requests
import csv
from kafka_broker import send_message
from src.utils.logger import get_logger

class RealTimeDataIngestion:
    def __init__(self):
        self.logger = get_logger('RealTimeDataIngestion')

    def collect_phishing_urls(self):
        """Fetch real-time phishing URLs from API and push to Kafka."""
        api_url = "https://openphish.com/feed.txt"  # Example source for real-time phishing URLs
        response = requests.get(api_url)
        urls = response.text.splitlines()
        
        for url in urls:
            self.logger.info(f"Collecting URL: {url}")
            send_message('real_time_urls', {'url': url})

    def start_real_time_collection(self):
        """Collect URLs every minute from various real-time sources."""
        while True:
            self.collect_phishing_urls()
            time.sleep(60)  # Collect URLs every minute

import time
import requests
import tweepy
from hdfs import InsecureClient  # HDFS client for Python
from kafka_broker import send_message
from src.utils.logger import get_logger
import redis
import logging
import subprocess
import os
import random
import zipfile
import io

# Configure logging to write to a file
logging.basicConfig(
    filename='real_time_data_ingestion.log',  # Log file name
    level=logging.INFO,  # Set the log level to INFO
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class RealTimeDataIngestion:
    def __init__(self):
        self.logger = get_logger('RealTimeDataIngestion')
        self.redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)  # Redis to track processed URLs
        self.hdfs_client = InsecureClient('http://localhost:9000', user='hadoop_user')  # HDFS client
        self.urls_to_store = []  # Store URLs collected from all sources

    def is_new_url(self, url):
        """Check if the URL has been processed before."""
        if not self.redis_client.exists(url):
            self.logger.debug(f"New URL detected: {url}")  # Use DEBUG for individual URL logs
            return True
        else:
            self.logger.debug(f"URL already processed: {url}")
            return False

    def mark_url_processed(self, url):
        """Mark a URL as processed by adding it to Redis."""
        self.redis_client.set(url, 1)  # Store the URL with an arbitrary value of 1
        self.logger.debug(f"URL marked as processed in Redis: {url}")

    def append_to_hdfs(self):
        """Append collected URLs to HDFS using HDFS CLI."""
        hdfs_path = "/phishing_urls/collected_urls.txt"
        local_file = "/tmp/collected_urls.txt"  # Temporary file to write URLs before appending

        try:
            # Ensure we have a balanced number of benign and malicious URLs
            benign_urls = [entry for entry in self.urls_to_store if entry['status'] == 0]
            malicious_urls = [entry for entry in self.urls_to_store if entry['status'] == 1]

            # Make sure the number of benign and malicious URLs is balanced
            num_to_store = min(len(benign_urls), len(malicious_urls))
            balanced_urls = benign_urls[:num_to_store] + malicious_urls[:num_to_store]
            random.shuffle(balanced_urls)  # Shuffle to avoid any patterns in the data

            self.logger.info(f"Balanced URLs to append: {len(balanced_urls)}")

            if not balanced_urls:
                self.logger.info("No new balanced URLs to append.")
                return  # Skip appending if no balanced URLs are available

            # Write collected URLs to a local temporary file
            with open(local_file, "w") as f:
                for entry in balanced_urls:
                    url = entry['url']
                    status = entry['status']
                    f.write(f"{url}, {status}\n")  # Write URL and its status (0 for benign, 1 for malicious)

            # Use the HDFS CLI to append the local file to the HDFS file
            cmd = f"hdfs dfs -appendToFile {local_file} {hdfs_path}"
            subprocess.run(cmd, shell=True, check=True)

            self.logger.info(f"Appended {len(balanced_urls)} balanced URLs to HDFS.")
            
            # Clean up the local temporary file after successfully appending
            os.remove(local_file)

        except subprocess.CalledProcessError as e:
            self.logger.error(f"Error appending URLs to HDFS using CLI: {str(e)}")
        except Exception as e:
            self.logger.error(f"Unexpected error: {str(e)}")
        finally:
            # Clear the URLs list after storing them in HDFS
            self.urls_to_store = []

    def fetch_benign_urls_from_majestic(self):
        """Fetch benign URLs from Majestic Million and process new ones."""
        api_url = "https://downloads.majestic.com/majestic_million.csv"
        try:
            self.logger.info("Fetching benign URLs from Majestic Million...")
            response = requests.get(api_url, timeout=10)
            response.raise_for_status()
            lines = response.text.splitlines()[1:101]  # Skip header, fetch 100 benign URLs

            for line in lines:
                url = line.split(',')[2]  # Extract URL from the third column
                if self.is_new_url(url):
                    self.logger.debug(f"Collected new benign URL from Majestic Million: {url}")
                    self.urls_to_store.append({'url': url, 'status': 0})  # Mark as benign (0)
                    self.mark_url_processed(url)
        except requests.RequestException as e:
            self.logger.error(f"Error fetching benign URLs from Majestic Million: {str(e)}")

    def fetch_benign_urls_from_umbrella(self):
        """Fetch benign URLs from Umbrella, extract CSV from ZIP, and process new ones."""
        api_url = "http://s3-us-west-1.amazonaws.com/umbrella-static/top-1m.csv.zip"
        try:
            self.logger.info("Fetching benign URLs from Umbrella...")
            response = requests.get(api_url, timeout=10)
            response.raise_for_status()
    
            # Open the ZIP file and extract the CSV
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                # Assuming there's only one CSV file inside
                with z.open(z.namelist()[0]) as csvfile:
                    urls = csvfile.read().decode('utf-8').splitlines()[1:101]  # Skip header, fetch 100 benign URLs
    
                    for line in urls:
                        url = line.split(',')[0]  # Since there's only one column
                        if self.is_new_url(url):
                            self.logger.debug(f"Collected new benign URL from Umbrella: {url}")
                            self.urls_to_store.append({'url': url, 'status': 0})  # Mark as benign (0)
                            self.mark_url_processed(url)
    
        except zipfile.BadZipFile as e:
            self.logger.error(f"Failed to unzip Umbrella dataset: {str(e)}")
        except requests.RequestException as e:
            self.logger.error(f"Error fetching benign URLs from Umbrella: {str(e)}")

    def fetch_malicious_urls(self):
        """Fetch malicious URLs from various sources."""
        self.fetch_phishing_urls_from_openphish()
        self.fetch_phishing_urls_from_cybercrime_tracker()
        self.fetch_phishing_urls_from_urlhaus()

    def update_local_db(self):
        """Update local database by writing collected URLs to HDFS."""
        self.logger.info("Updating local URL database with collected URLs.")
        self.append_to_hdfs()  # Push collected URLs to HDFS

    def start_real_time_collection(self):
        """Continuously collect URLs from multiple sources and store them in HDFS."""
        while True:
            self.logger.info("Starting new round of URL collection...")

            # Fetch benign URLs
            self.fetch_benign_urls_from_majestic()
            self.fetch_benign_urls_from_umbrella()

            # Fetch phishing URLs (malicious)
            self.fetch_malicious_urls()

            # Update local database (push to HDFS)
            self.update_local_db()

            time.sleep(60)  # Collect URLs every minute

if __name__ == "__main__":
    ingestion_service = RealTimeDataIngestion()
    ingestion_service.start_real_time_collection()

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

# Configure logging to write to a file
logging.basicConfig(
    filename='real_time_data_ingestion.log',  # Log file name
    level=logging.INFO,  # Log level
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class RealTimeDataIngestion:
    def __init__(self):
        self.logger = get_logger('RealTimeDataIngestion')
        self.redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)  # Redis to track processed URLs
        self.twitter_api = self.initialize_twitter_api()
        self.hdfs_client = InsecureClient('http://localhost:9000', user='hadoop_user')  # HDFS client
        self.urls_to_store = []  # Store URLs collected from all sources

    def initialize_twitter_api(self):
        """Initialize the Twitter API client using tweepy for real-time phishing URL collection."""
        api_key = "GFuzyaJ9WbXM2GyLGomXsyEAh"
        api_secret_key = "IEP3mGepl2zUVQsau3TyXz6mhV7bdreLamPKbdwcyEDAe2azMs"
        bearer_token = "AAAAAAAAAAAAAAAAAAAAAM0SwgEAAAAAb%2FNx6I6JHcAtwIHNF8ZQXHCMP%2F0%3DHyiBs0bdHfkfdiOSPJORHG51NNFtooVNdvqcPab9Ua3QnBsJLK"
        
        client = tweepy.Client(bearer_token=bearer_token)
        return client

    def is_new_url(self, url):
        """Check if the URL has been processed before."""
        if not self.redis_client.exists(url):
            self.logger.info(f"New URL detected: {url}")
            return True
        else:
            self.logger.info(f"URL already processed: {url}")
            return False

    def mark_url_processed(self, url):
        """Mark a URL as processed by adding it to Redis."""
        self.redis_client.set(url, 1)  # Store the URL with an arbitrary value of 1
        self.logger.info(f"URL marked as processed in Redis: {url}")

    def append_to_hdfs(self):
        """Append collected URLs to HDFS using HDFS CLI."""
        hdfs_path = "/phishing_urls/collected_urls.txt"
        local_file = "/tmp/collected_urls.txt"  # Temporary file to write URLs before appending

        try:
            # Log the URLs being appended
            self.logger.info(f"URLs to append: {self.urls_to_store}")

            if not self.urls_to_store:
                self.logger.info("No new URLs to append.")
                return  # Skip appending if no URLs are available

            # Write collected URLs to a local temporary file
            with open(local_file, "w") as f:
                for entry in self.urls_to_store:
                    url = entry['url']
                    status = entry['status']
                    f.write(f"{url}, {status}\n")  # Write URL and its status (0 for benign, 1 for malicious)

            # Use the HDFS CLI to append the local file to the HDFS file
            cmd = f"hdfs dfs -appendToFile {local_file} {hdfs_path}"
            subprocess.run(cmd, shell=True, check=True)

            self.logger.info(f"Appended {len(self.urls_to_store)} URLs to HDFS.")
            
            # Clean up the local temporary file after successfully appending
            os.remove(local_file)

        except subprocess.CalledProcessError as e:
            self.logger.error(f"Error appending URLs to HDFS using CLI: {str(e)}")
        except Exception as e:
            self.logger.error(f"Unexpected error: {str(e)}")
        finally:
            # Clear the URLs list after storing them in HDFS
            self.urls_to_store = []

    def fetch_phishing_urls_from_openphish(self):
        """Fetch phishing URLs from OpenPhish and process new ones."""
        api_url = "https://openphish.com/feed.txt"
        try:
            self.logger.info("Fetching data from OpenPhish...")
            response = requests.get(api_url, timeout=10)
            response.raise_for_status()
            urls = response.text.splitlines()

            for url in urls:
                if self.is_new_url(url):
                    self.logger.info(f"Collected new URL from OpenPhish: {url}")
                    send_message('real_time_urls', {'url': url, 'status': 1})  # Mark as malicious (1)
                    self.urls_to_store.append({'url': url, 'status': 1})
                    self.mark_url_processed(url)
        except requests.RequestException as e:
            self.logger.error(f"Error fetching data from OpenPhish: {str(e)}")

    def fetch_phishing_urls_from_cybercrime_tracker(self):
        """Fetch phishing URLs from Cybercrime Tracker and process new ones."""
        api_url = "https://cybercrime-tracker.net/all.php"
        try:
            self.logger.info("Fetching data from Cybercrime Tracker...")
            response = requests.get(api_url, timeout=10)
            response.raise_for_status()
            urls = response.text.splitlines()

            for url in urls:
                if self.is_new_url(url) and url:  # Ensure URL is valid and non-empty
                    if not url.startswith("http://") and not url.startswith("https://"):
                        url = "http://" + url  # Add the prefix if missing
                    self.logger.info(f"Collected new URL from Cybercrime Tracker: {url}")
                    send_message('real_time_urls', {'url': url, 'status': 1})  # Mark as malicious (1)
                    self.urls_to_store.append({'url': url, 'status': 1})
                    self.mark_url_processed(url)
        except requests.RequestException as e:
            self.logger.error(f"Error fetching data from Cybercrime Tracker: {str(e)}")

    def fetch_phishing_urls_from_urlhaus(self):
        """Fetch phishing URLs from URLHaus and process new ones."""
        api_url = "https://urlhaus.abuse.ch/downloads/csv_recent/"
        try:
            self.logger.info("Fetching data from URLHaus...")
            response = requests.get(api_url, timeout=10)
            response.raise_for_status()
            urls = response.text.splitlines()
    
            for line in urls[9:]:  # Skip header and comments
                try:
                    fields = line.split('","')  # Split by CSV format
                    if len(fields) > 2:  # Ensure we have enough fields
                        url = fields[2].replace('"', '')  # Extract URL field and remove quotes
                        status = 1 if fields[3] == 'online' else 0  # Mark as 1 for malicious, 0 for benign
                        if self.is_new_url(url):
                            self.logger.info(f"Collected new URL from URLHaus: {url}")
                            send_message('real_time_urls', {'url': url, 'status': status})
                            self.urls_to_store.append({'url': url, 'status': status})
                            self.mark_url_processed(url)
                except IndexError:
                    self.logger.error("Error parsing URLHaus data")
        except requests.RequestException as e:
            self.logger.error(f"Error fetching data from URLHaus: {str(e)}")

    def fetch_urls_from_abuseipdb(self):
        """Fetch URLs from AbuseIPDB and process new ones."""
        api_url = "https://api.abuseipdb.com/api/v2/blacklist"
        headers = {'Key': '5999a5f9ad99fad0a5b31b20910a82470f52c02bb382ecbd041deb8f5e8c6a3bd7b2561fcefb49ec'}
        try:
            self.logger.info("Fetching data from AbuseIPDB...")
            response = requests.get(api_url, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            urls = [item['ipAddress'] for item in data['data']]  # Process IP addresses

            for url in urls:
                if self.is_new_url(url):
                    self.logger.info(f"Collected new URL from AbuseIPDB: {url}")
                    send_message('real_time_urls', {'url': url, 'status': 1})  # Mark as malicious (1)
                    self.urls_to_store.append({'url': url, 'status': 1})
                    self.mark_url_processed(url)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                self.logger.error(f"Rate limit hit for AbuseIPDB. Skipping this round...")
            else:
                self.logger.error(f"Error fetching data from AbuseIPDB: {str(e)}")

    def update_local_db(self):
        """Update local database by writing collected URLs to HDFS."""
        self.logger.info("Updating local URL database with collected URLs.")
        self.append_to_hdfs()  # Push collected URLs to HDFS

    def start_real_time_collection(self):
        """Continuously collect URLs from multiple sources and store them in HDFS."""
        while True:
            self.logger.info("Starting new round of URL collection...")

            # Fetch phishing URLs from various sources
            self.fetch_phishing_urls_from_openphish()
            self.fetch_phishing_urls_from_cybercrime_tracker()
            self.fetch_phishing_urls_from_urlhaus()
            self.fetch_urls_from_abuseipdb()

            # Update local database (push to HDFS)
            self.update_local_db()

            time.sleep(60)  # Collect URLs every minute

if __name__ == "__main__":
    ingestion_service = RealTimeDataIngestion()
    ingestion_service.start_real_time_collection()

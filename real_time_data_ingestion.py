import time
import requests
import tweepy
# from hdfs import InsecureClient  # HDFS client for Python (Commented out for debugging)
from kafka_broker import send_message
from src.utils.logger import get_logger
import redis
import logging

# Configure logging to write to a file instead of console
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
        # self.hdfs_client = InsecureClient('http://localhost:9000', user='hadoop_user')  # HDFS client

    def initialize_twitter_api(self):
        """Initialize the Twitter API client using tweepy for real-time phishing URL collection."""
        api_key = "GFuzyaJ9WbXM2GyLGomXsyEAh"
        api_secret_key = "IEP3mGepl2zUVQsau3TyXz6mhV7bdreLamPKbdwcyEDAe2azMs"
        bearer_token = "AAAAAAAAAAAAAAAAAAAAAM0SwgEAAAAAb%2FNx6I6JHcAtwIHNF8ZQXHCMP%2F0%3DHyiBs0bdHfkfdiOSPJORHG51NNFtooVNdvqcPab9Ua3QnBsJLK"
        
        client = tweepy.Client(bearer_token=bearer_token)
        return client

    def is_new_url(self, url):
        """Check if the URL has been processed before."""
        return not self.redis_client.exists(url)

    def mark_url_processed(self, url):
        """Mark a URL as processed by adding it to Redis."""
        self.redis_client.set(url, 1)  # Store the URL

    def append_to_hdfs(self, url):
        """Append the new URL to a file in HDFS (Commented out for debugging)."""
        # hdfs_path = "/phishing_urls/collected_urls.txt"
        # with self.hdfs_client.write(hdfs_path, append=True, encoding='utf-8') as writer:
        #     writer.write(f"{url}\n")
        # self.logger.info(f"Appended URL to HDFS: {url}")
        pass

    def fetch_phishing_urls_from_openphish(self):
        """Fetch phishing URLs from OpenPhish and process new ones."""
        api_url = "https://openphish.com/feed.txt"
        try:
            response = requests.get(api_url, timeout=10)
            response.raise_for_status()
            urls = response.text.splitlines()

            for url in urls:
                if self.is_new_url(url):
                    self.logger.info(f"Collected new URL from OpenPhish: {url}")
                    send_message('real_time_urls', {'url': url})
                    # self.append_to_hdfs(url)  # Commented out for debugging
                    self.mark_url_processed(url)
        except requests.RequestException as e:
            self.logger.error(f"Error fetching data from OpenPhish: {str(e)}")

    def fetch_phishing_urls_from_cybercrime_tracker(self):
        """Fetch phishing URLs from Cybercrime Tracker and process new ones."""
        api_url = "https://cybercrime-tracker.net/all.php"
        try:
            response = requests.get(api_url, timeout=10)
            response.raise_for_status()
            urls = response.text.splitlines()

            for url in urls:
                if self.is_new_url(url) and url:  # Ensure URL is valid and non-empty
                    self.logger.info(f"Collected new URL from Cybercrime Tracker: {url}")
                    send_message('real_time_urls', {'url': url})
                    # self.append_to_hdfs(url)  # Commented out for debugging
                    self.mark_url_processed(url)
        except requests.RequestException as e:
            self.logger.error(f"Error fetching data from Cybercrime Tracker: {str(e)}")

    def fetch_phishing_urls_from_urlhaus(self):
        """Fetch phishing URLs from URLHaus and process new ones."""
        api_url = "https://urlhaus.abuse.ch/downloads/csv/"
        try:
            response = requests.get(api_url, timeout=10)
            response.raise_for_status()
            urls = response.text.splitlines()

            for line in urls[9:]:  # Skip header
                try:
                    fields = line.split(",")
                    if len(fields) > 2:  # Ensure we have enough fields
                        url = fields[2].replace('"', '')
                        if self.is_new_url(url):
                            self.logger.info(f"Collected new URL from URLHaus: {url}")
                            send_message('real_time_urls', {'url': url})
                            # self.append_to_hdfs(url)  # Commented out for debugging
                            self.mark_url_processed(url)
                except IndexError:
                    self.logger.error("Error parsing URLHaus data")
        except requests.RequestException as e:
            self.logger.error(f"Error fetching data from URLHaus: {str(e)}")

    def fetch_urls_from_twitter(self):
        """Collect phishing URLs from Twitter using specific keywords and process new ones."""
        try:
            search_query = "phishing OR malware OR malicious URL filter:links"
            tweets = self.twitter_api.search_recent_tweets(query=search_query, tweet_fields=['entities'], max_results=100)
            
            for tweet in tweets.data:
                if 'urls' in tweet.entities:
                    for url in tweet.entities['urls']:
                        expanded_url = url['expanded_url']
                        if self.is_new_url(expanded_url):
                            self.logger.info(f"Collected new URL from Twitter: {expanded_url}")
                            send_message('real_time_urls', {'url': expanded_url})
                            #self.append_to_hdfs(expanded_url)  # Commented out for debugging
                            self.mark_url_processed(expanded_url)
        except Exception as e:
            self.logger.error(f"Error fetching data from Twitter: {str(e)}")

    def fetch_urls_from_threatfox(self):
        """Fetch phishing URLs from ThreatFox and process new ones."""
        api_url = "https://threatfox.abuse.ch/export/csv/urls/recent/"
        try:
            response = requests.get(api_url, timeout=10)
            response.raise_for_status()
            urls = response.text.splitlines()

            for line in urls[1:]:  # Skip header
                try:
                    fields = line.split(",")
                    if len(fields) > 1:  # Ensure we have enough fields
                        url = fields[1].replace('"', '')
                        if self.is_new_url(url):
                            self.logger.info(f"Collected new URL from ThreatFox: {url}")
                            send_message('real_time_urls', {'url': url})
                            # self.append_to_hdfs(url)  # Commented out for debugging
                            self.mark_url_processed(url)
                except IndexError:
                    self.logger.error("Error parsing ThreatFox data")
        except requests.RequestException as e:
            self.logger.error(f"Error fetching data from ThreatFox: {str(e)}")

    def update_local_db(self):
        """Update local database by sending URLs to Kafka and storing them in HDFS (Commented out)."""
        self.logger.info("Updating local URL database with collected URLs.")

    def start_real_time_collection(self):
        """Continuously collect URLs from multiple sources and store them in HDFS."""
        while True:
            self.logger.info("Starting new round of URL collection...")

            # Fetch phishing URLs from various sources
            self.fetch_phishing_urls_from_openphish()
            self.fetch_phishing_urls_from_cybercrime_tracker()
            self.fetch_phishing_urls_from_urlhaus()
            self.fetch_urls_from_twitter()
            self.fetch_urls_from_threatfox()

            # Update local database (Hadoop commented out for debugging)
            self.update_local_db()

            time.sleep(60)  # Collect URLs every minute

if __name__ == "__main__":
    ingestion_service = RealTimeDataIngestion()
    ingestion_service.start_real_time_collection()

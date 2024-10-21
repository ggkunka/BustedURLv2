import time
import requests
import tweepy
from hdfs import InsecureClient  # HDFS client for Python
from kafka_broker import send_message
from src.utils.logger import get_logger
import redis

class RealTimeDataIngestion:
    def __init__(self):
        self.logger = get_logger('RealTimeDataIngestion')
        self.redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)  # Redis to track processed URLs
        self.twitter_api = self.initialize_twitter_api()
        self.hdfs_client = InsecureClient('http://localhost:9000', user='hadoop_user')  # HDFS client

    def initialize_twitter_api(self):
        """Initialize the Twitter API client using tweepy for real-time phishing URL collection."""
        api_key = "YOUR_TWITTER_API_KEY"
        api_secret_key = "YOUR_TWITTER_API_SECRET"
        access_token = "YOUR_TWITTER_ACCESS_TOKEN"
        access_token_secret = "YOUR_TWITTER_ACCESS_TOKEN_SECRET"
        
        auth = tweepy.OAuth1UserHandler(api_key, api_secret_key, access_token, access_token_secret)
        return tweepy.API(auth)

    def is_new_url(self, url):
        """Check if the URL has been processed before."""
        return not self.redis_client.exists(url)

    def mark_url_processed(self, url):
        """Mark a URL as processed by adding it to Redis."""
        self.redis_client.set(url, 1)  # Store the URL

    def append_to_hdfs(self, url):
        """Append the new URL to a file in HDFS."""
        hdfs_path = "/phishing_urls/collected_urls.txt"
        with self.hdfs_client.write(hdfs_path, append=True, encoding='utf-8') as writer:
            writer.write(f"{url}\n")
        self.logger.info(f"Appended URL to HDFS: {url}")

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
                    self.append_to_hdfs(url)  # Store URL in HDFS
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
                    self.append_to_hdfs(url)  # Store URL in HDFS
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
                            self.append_to_hdfs(url)  # Store URL in HDFS
                            self.mark_url_processed(url)
                except IndexError:
                    self.logger.error("Error parsing URLHaus data")
        except requests.RequestException as e:
            self.logger.error(f"Error fetching data from URLHaus: {str(e)}")

    def fetch_urls_from_twitter(self):
        """Collect phishing URLs from Twitter using specific keywords and process new ones."""
        try:
            search_query = "phishing OR malware OR malicious URL filter:links"
            tweets = self.twitter_api.search_recent_tweets(q=search_query, count=100, result_type="recent", lang="en")
            
            for tweet in tweets:
                for url in tweet.entities['urls']:
                    expanded_url = url['expanded_url']
                    if self.is_new_url(expanded_url):
                        self.logger.info(f"Collected new URL from Twitter: {expanded_url}")
                        send_message('real_time_urls', {'url': expanded_url})
                        self.append_to_hdfs(expanded_url)  # Store URL in HDFS
                        self.mark_url_processed(expanded_url)
        except Exception as e:
            self.logger.error(f"Error fetching data from Twitter: {str(e)}")

    def fetch_urls_from_dark_web(self):
        """Collect URLs from the dark web via DarkOwl API (or similar service) and process new ones."""
        api_url = "https://api.darkowl.com/dark-intel/search"
        headers = {
            'Authorization': 'Bearer YOUR_DARKOWL_API_TOKEN',
            'Content-Type': 'application/json',
        }
        payload = {
            "query": "phishing OR malware",
            "limit": 100
        }
        
        try:
            response = requests.post(api_url, json=payload, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            urls = [item['url'] for item in data['data'] if 'url' in item]

            for url in urls:
                if self.is_new_url(url):
                    self.logger.info(f"Collected new URL from Dark Web: {url}")
                    send_message('real_time_urls', {'url': url})
                    self.append_to_hdfs(url)  # Store URL in HDFS
                    self.mark_url_processed(url)
        except requests.RequestException as e:
            self.logger.error(f"Error fetching data from Dark Web: {str(e)}")

    def update_local_db(self):
        """Update local database by sending URLs to Kafka and storing them in HDFS."""
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
            self.fetch_urls_from_dark_web()

            # Update local database
            self.update_local_db()

            time.sleep(60)  # Collect URLs every minute

if __name__ == "__main__":
    ingestion_service = RealTimeDataIngestion()
    ingestion_service.start_real_time_collection()

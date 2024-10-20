import time
import requests
import tweepy
from kafka_broker import send_message
from src.utils.logger import get_logger

class RealTimeDataIngestion:
    def __init__(self):
        self.logger = get_logger('RealTimeDataIngestion')
        self.twitter_api = self.initialize_twitter_api()

    def initialize_twitter_api(self):
        """Initialize the Twitter API client using tweepy for real-time phishing URL collection."""
        api_key = "YOUR_TWITTER_API_KEY"
        api_secret_key = "YOUR_TWITTER_API_SECRET"
        access_token = "YOUR_TWITTER_ACCESS_TOKEN"
        access_token_secret = "YOUR_TWITTER_ACCESS_TOKEN_SECRET"
        
        auth = tweepy.OAuth1UserHandler(api_key, api_secret_key, access_token, access_token_secret)
        return tweepy.API(auth)

    def fetch_phishing_urls_from_openphish(self):
        """Fetch phishing URLs from OpenPhish."""
        api_url = "https://openphish.com/feed.txt"  # OpenPhish phishing feed
        try:
            response = requests.get(api_url)
            response.raise_for_status()
            urls = response.text.splitlines()

            for url in urls:
                self.logger.info(f"Collected URL from OpenPhish: {url}")
                send_message('real_time_urls', {'url': url})
        except requests.RequestException as e:
            self.logger.error(f"Error fetching data from OpenPhish: {str(e)}")

    def fetch_phishing_urls_from_phishtank(self):
        """Fetch phishing URLs from PhishTank."""
        api_url = "https://data.phishtank.com/data/online-valid.csv"
        try:
            response = requests.get(api_url)
            response.raise_for_status()
            urls = response.text.splitlines()

            for url in urls[1:]:  # Skip the header line
                self.logger.info(f"Collected URL from PhishTank: {url}")
                send_message('real_time_urls', {'url': url})
        except requests.RequestException as e:
            self.logger.error(f"Error fetching data from PhishTank: {str(e)}")

    def fetch_phishing_urls_from_urlhaus(self):
        """Fetch phishing URLs from URLHaus."""
        api_url = "https://urlhaus.abuse.ch/downloads/csv/"
        try:
            response = requests.get(api_url)
            response.raise_for_status()
            urls = response.text.splitlines()

            for line in urls[9:]:  # Skip headers
                url = line.split(",")[2].replace('"', '')  # Extract URL from CSV
                self.logger.info(f"Collected URL from URLHaus: {url}")
                send_message('real_time_urls', {'url': url})
        except requests.RequestException as e:
            self.logger.error(f"Error fetching data from URLHaus: {str(e)}")

    def fetch_urls_from_twitter(self):
        """Collect phishing URLs from Twitter using specific keywords."""
        try:
            search_query = "phishing OR malware OR malicious URL filter:links"
            tweets = self.twitter_api.search(q=search_query, count=100, result_type="recent", lang="en")
            
            for tweet in tweets:
                for url in tweet.entities['urls']:
                    expanded_url = url['expanded_url']
                    self.logger.info(f"Collected URL from Twitter: {expanded_url}")
                    send_message('real_time_urls', {'url': expanded_url})
        except Exception as e:
            self.logger.error(f"Error fetching data from Twitter: {str(e)}")

    def fetch_urls_from_dark_web(self):
        """Collect URLs from the dark web via DarkOwl API (or similar service)."""
        api_url = "https://api.darkowl.com/dark-intel/search"  # Example: DarkOwl API
        headers = {
            'Authorization': 'Bearer YOUR_DARKOWL_API_TOKEN',
            'Content-Type': 'application/json',
        }
        payload = {
            "query": "phishing OR malware",
            "limit": 100
        }
        
        try:
            response = requests.post(api_url, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()
            urls = [item['url'] for item in data['data'] if 'url' in item]

            for url in urls:
                self.logger.info(f"Collected URL from Dark Web: {url}")
                send_message('real_time_urls', {'url': url})
        except requests.RequestException as e:
            self.logger.error(f"Error fetching data from Dark Web: {str(e)}")

    def update_local_db(self):
        """Update local database by sending URLs to Kafka."""
        self.logger.info("Updating local URL database with collected URLs.")

    def start_real_time_collection(self):
        """Continuously collect URLs from multiple sources."""
        while True:
            self.logger.info("Starting new round of URL collection...")

            # Fetch phishing URLs from various sources
            self.fetch_phishing_urls_from_openphish()
            self.fetch_phishing_urls_from_phishtank()
            self.fetch_phishing_urls_from_urlhaus()
            self.fetch_urls_from_twitter()
            self.fetch_urls_from_dark_web()

            # Update local database
            self.update_local_db()

            time.sleep(60)  # Collect URLs every minute

if __name__ == "__main__":
    ingestion_service = RealTimeDataIngestion()
    ingestion_service.start_real_time_collection()

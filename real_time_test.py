import requests  # Optionally for a REST interface
import logging
import pandas as pd
import random
import time
from src.main import process_url  # Assuming you are calling the process_url directly
from ids_ips.integration import IDS_IPS_Integration

logger = logging.getLogger("RealTimeTest")

def load_test(dataset, ids_ips, num_urls=500, delay=0.1):
    """Simulate a high load of URL processing for performance testing."""
    logger.info(f"Starting load test with {num_urls} URLs at {delay} second intervals.")

    url_chunk = dataset['url'].values[:num_urls]
    for i, url in enumerate(url_chunk):
        process_url(url, ids_ips)
        if i % 50 == 0:
            logger.info(f"Processed {i} URLs.")
        time.sleep(delay)

    logger.info(f"Load test completed for {num_urls} URLs.")

def run_real_time_test(dataset, num_urls=500):
    """Run a real-time URL test, using the given dataset for known URL classifications."""
    ids_ips = IDS_IPS_Integration()  # Initialize IDS/IPS
    url_chunk = dataset['url'].sample(num_urls).values  # Random sample of URLs

    for url in url_chunk:
        process_url(url, ids_ips)
        logger.info(f"Processed URL: {url}")
        time.sleep(0.5)  # Adjust the delay as needed to simulate real-time analysis

def real_time_menu():
    """Provide a manual interface for running real-time tests."""
    dataset = pd.read_csv('/tmp/collected_urls.txt', header=None, names=['url', 'label'])

    while True:
        print("\nReal-Time Testing Menu")
        print("1. Run Basic Real-Time Test")
        print("2. Run Load Test")
        print("3. Exit")
        choice = input("Enter your choice: ")

        if choice == "1":
            num_urls = int(input("Enter the number of URLs for the real-time test: "))
            run_real_time_test(dataset, num_urls)
        elif choice == "2":
            num_urls = int(input("Enter the number of URLs for the load test: "))
            delay = float(input("Enter the delay between URL processing (seconds): "))
            load_test(dataset, IDS_IPS_Integration(), num_urls, delay)
        elif choice == "3":
            print("Exiting Real-Time Test.")
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    real_time_menu()

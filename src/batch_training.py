import time
import multiprocessing as mp
from src.ensemble_model import EnsembleModel
from src.utils.hdfs_helper import fetch_batches_from_hdfs
from src.utils.preprocessing import preprocess_urls
from src.utils.logger import get_logger

class BatchTrainingPipeline:
    def __init__(self, batch_size=1000, num_workers=4):
        self.logger = get_logger('BatchTrainingPipeline')
        self.model = EnsembleModel()
        self.batch_size = batch_size
        self.num_workers = num_workers

    def train_model_on_batch(self, batch_urls):
        # Preprocess the URLs (vectorization or embeddings)
        features, labels = preprocess_urls(batch_urls)
        self.model.train_on_batch(features, labels)

    def process_batch(self, batch_urls):
        """Train the model on a single batch of URLs."""
        self.logger.info(f"Processing batch of size {len(batch_urls)}")
        self.train_model_on_batch(batch_urls)

    def fetch_and_process_batches(self):
        while True:
            self.logger.info("Fetching batches from HDFS")
            # Fetch new data from HDFS in batches
            batches = fetch_batches_from_hdfs(self.batch_size)
            if not batches:
                self.logger.info("No new batches to process.")
                time.sleep(60)  # Sleep for 1 minute before checking again
                continue

            # Multiprocessing to train in parallel
            with mp.Pool(self.num_workers) as pool:
                pool.map(self.process_batch, batches)

    def start(self):
        self.logger.info("Starting batch training pipeline...")
        self.fetch_and_process_batches()

if __name__ == "__main__":
    # Utilize 24 cores by using 12 workers (each worker can use multiple threads)
    pipeline = BatchTrainingPipeline(num_workers=12)
    pipeline.start()

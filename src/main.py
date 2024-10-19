import multiprocessing as mp
from run_scalability_test import run_real_time_test
from src.utils.logger import get_logger

logger = get_logger('Main')

if __name__ == "__main__":
    logger.info("Starting BustedURL system with real-time processing...")
    
    # Create process for real-time testing
    real_time_test_process = mp.Process(target=run_real_time_test)

    # Start the real-time testing
    real_time_test_process.start()

    # Join the processes to wait for completion (if necessary)
    real_time_test_process.join()

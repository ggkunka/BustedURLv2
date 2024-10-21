import subprocess
import os
from src.utils.logger import get_logger

logger = get_logger('HDFSHelper')

def fetch_batches_from_hdfs(batch_size):
    """Fetch batches of URLs from HDFS."""
    hdfs_path = "/phishing_urls/collected_urls.txt"
    local_file = "/tmp/hdfs_urls.txt"
    
    try:
        # Download the file from HDFS to a local temp file
        cmd = f"hdfs dfs -get {hdfs_path} {local_file}"
        subprocess.run(cmd, shell=True, check=True)
        
        # Read the local file and split into batches
        with open(local_file, 'r') as f:
            urls = f.readlines()
        
        # Split the URLs into batches
        batches = [urls[i:i + batch_size] for i in range(0, len(urls), batch_size)]
        
        # Clean up local file after reading
        os.remove(local_file)
        
        logger.info(f"Fetched {len(batches)} batches from HDFS")
        return batches
    except subprocess.CalledProcessError as e:
        logger.error(f"Error fetching batches from HDFS: {str(e)}")
        return []

from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np
from src.utils.logger import get_logger

logger = get_logger('Preprocessing')

def preprocess_urls(urls):
    """Preprocess URLs for training."""
    # Assuming labels are provided in the form (url, label)
    urls, labels = zip(*[(url.strip(), label) for url, label in urls if url])
    
    # Vectorize the URLs using TF-IDF
    vectorizer = TfidfVectorizer(analyzer='char', ngram_range=(3, 5))
    X = vectorizer.fit_transform(urls)
    
    # Convert labels to numpy array
    y = np.array(labels)
    
    logger.info(f"Preprocessed {len(urls)} URLs.")
    return X, y

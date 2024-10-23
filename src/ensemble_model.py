import torch
import numpy as np
import logging
import os
import joblib
from transformers import BertModel, RobertaModel, DistilBertModel, XLNetModel
from sklearn.ensemble import StackingClassifier
from sklearn.linear_model import SGDClassifier, PassiveAggressiveClassifier
from sklearn.metrics import (accuracy_score, precision_score, recall_score,
                             f1_score, roc_auc_score, confusion_matrix)
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.utils import resample
from multiprocessing import Pool
import tracemalloc  # For memory profiling
from config.app_config import USE_XLNET

class EnsembleModel:
    def __init__(self):
        # Initialize transformer models for feature extraction
        self.transformer_models = [
            BertModel.from_pretrained('bert-base-uncased'),
            RobertaModel.from_pretrained('roberta-base'),
            DistilBertModel.from_pretrained('distilbert-base-uncased')
        ]

        if USE_XLNET:
            self.transformer_models.append(XLNetModel.from_pretrained('xlnet-base-cased'))

        # Use SGDClassifier and PassiveAggressiveClassifier for online learning
        self.online_classifiers = {
            'sgd': SGDClassifier(loss='log_loss', max_iter=1000, tol=1e-3),
            'passive_aggressive': PassiveAggressiveClassifier(max_iter=1000, tol=1e-3)
        }

        # Vectorizer for transforming URLs into features, limiting max features for memory efficiency
        self.vectorizer = TfidfVectorizer(analyzer='char', ngram_range=(2, 3), max_features=500)

    def stratified_sampling(self, X, y):
        """Perform stratified sampling to balance classes."""
        malicious = X[y == 1]
        benign = X[y == 0]

        if len(malicious) > len(benign):
            malicious_downsampled = resample(malicious, replace=False, n_samples=len(benign), random_state=42)
            X_balanced = np.concatenate([malicious_downsampled, benign])
            y_balanced = np.concatenate([np.ones(len(benign)), np.zeros(len(benign))])
        else:
            benign_downsampled = resample(benign, replace=False, n_samples=len(malicious), random_state=42)
            X_balanced = np.concatenate([malicious, benign_downsampled])
            y_balanced = np.concatenate([np.ones(len(malicious)), np.zeros(len(malicious))])

        return X_balanced, y_balanced

    def fit_online(self, X_batch, y_batch, classifier_type='sgd'):
        """Train the model on a batch of data with incremental/online learning."""
        logging.info(f"Received X_batch of type {type(X_batch)}")

        tracemalloc.start()
        start_snapshot = tracemalloc.take_snapshot()

        if isinstance(X_batch, np.ndarray):
            X_batch = X_batch.tolist()

        X_balanced, y_balanced = self.stratified_sampling(np.array(X_batch), np.array(y_batch))
        X_transformed = self.vectorizer.fit_transform(X_balanced)
        logging.info(f"Transformed X_batch into feature matrix of shape {X_transformed.shape}")

        classifier = self.online_classifiers.get(classifier_type)
        if classifier is not None:
            classifier.partial_fit(X_transformed, y_balanced, classes=np.unique(y_balanced))
            logging.info(f"Online learning with {classifier_type} classifier completed.")

        # Memory profiling after training
        end_snapshot = tracemalloc.take_snapshot()
        memory_diff = end_snapshot.compare_to(start_snapshot, 'lineno')
        for stat in memory_diff[:10]:
            logging.info(f"Memory usage during training: {stat}")

    def extract_features(self, X_batch):
        """Extract features using the vectorizer."""
        if isinstance(X_batch, str):
            X_batch = [X_batch]
        return self.vectorizer.transform(X_batch).toarray()

    def classify(self, features):
        """Classify the features into labels."""
        return self.online_classifiers['sgd'].predict(features)

    def classify_proba(self, features):
        """Return the probability estimates for the classes."""
        return self.online_classifiers['sgd'].predict_proba(features)

    def process_single_url(self, url):
        """Process a single URL: extract features and classify."""
        features = self.extract_features([url])
        prediction = self.classify(features)
        return features, prediction

    def process_urls_in_parallel(self, url_list):
        """Process URLs in parallel using multiprocessing and track memory usage."""
        tracemalloc.start()
        start_snapshot = tracemalloc.take_snapshot()

        with Pool() as pool:
            results = pool.map(self.process_single_url, url_list)
        features_list, predictions = zip(*results)

        end_snapshot = tracemalloc.take_snapshot()
        memory_diff = end_snapshot.compare_to(start_snapshot, 'lineno')
        for stat in memory_diff[:10]:
            logging.info(f"Memory usage during parallel processing: {stat}")

        return features_list, predictions

    def calculate_metrics(self, y_true, y_pred, y_pred_proba=None):
        """Calculate various evaluation metrics."""
        accuracy = accuracy_score(y_true, y_pred)
        precision = precision_score(y_true, y_pred, average='binary')
        recall = recall_score(y_true, y_pred, average='binary')
        f1 = f1_score(y_true, y_pred, average='binary')

        if y_pred_proba is not None and len(y_pred_proba.shape) > 1:
            roc_auc = roc_auc_score(y_true, y_pred_proba[:, 1])
        else:
            roc_auc = None

        conf_matrix = confusion_matrix(y_true, y_pred)
        tn, fp, fn, tp = conf_matrix.ravel()
        tpr = tp / (tp + fn)
        fpr = fp / (fp + tn)

        return {
            'accuracy': accuracy,
            'precision': precision,
            'recall': recall,
            'f1_score': f1,
            'roc_auc': roc_auc,
            'confusion_matrix': conf_matrix,
            'tp': tp,
            'tn': tn,
            'fp': fp,
            'fn': fn,
            'tpr': tpr,
            'fpr': fpr
        }

    def save_model(self, path="models/ensemble_model.pkl"):
        """Save the trained model and vectorizer to disk."""
        directory = os.path.dirname(path)
        if not os.path.exists(directory):
            os.makedirs(directory)
        
        joblib.dump({'model': self.online_classifiers, 'vectorizer': self.vectorizer}, path)
        logging.info(f"Model and vectorizer saved to {path}")

    def load_model(self, path="models/ensemble_model.pkl"):
        """Load the saved model and vectorizer from disk."""
        loaded_data = joblib.load(path)
        self.online_classifiers = loaded_data['model']
        self.vectorizer = loaded_data['vectorizer']

import torch
import numpy as np
import logging
import os
import joblib
from transformers import BertModel, RobertaModel, DistilBertModel, XLNetModel
from sklearn.ensemble import StackingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import (accuracy_score, precision_score, recall_score,
                             f1_score, roc_auc_score, confusion_matrix)
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.utils import resample
from multiprocessing import Pool
from config.app_config import USE_XLNET

class EnsembleModel:
    def __init__(self):
        # Initialize transformer models for feature extraction
        self.transformer_models = [
            BertModel.from_pretrained('bert-base-uncased'),
            RobertaModel.from_pretrained('roberta-base'),
            DistilBertModel.from_pretrained('distilbert-base-uncased')
        ]

        # Conditionally add XLNet based on configuration flag
        if USE_XLNET:
            self.transformer_models.append(XLNetModel.from_pretrained('xlnet-base-cased'))

        # Initialize the stacking classifier with multiple base models
        self.stacking_classifier = StackingClassifier(
            estimators=[
                ('lr', LogisticRegression()),
                ('svc', SVC(probability=True)),  # Enable probability for ROC AUC
                ('dt', DecisionTreeClassifier())
            ],
            final_estimator=LogisticRegression()
        )

        # Vectorizer for transforming URLs into features
        self.vectorizer = TfidfVectorizer(analyzer='char', ngram_range=(2, 3), max_features=1000)

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

    def fit(self, X_batch, y_batch):
        """Train the model on a batch of data with stratified sampling."""
        logging.info(f"Received X_batch of type {type(X_batch)}")

        if isinstance(X_batch, np.ndarray):
            X_batch = X_batch.tolist()
            logging.info(f"Converted X_batch from np.ndarray to list.")

        if not all(isinstance(x, str) for x in X_batch):
            raise ValueError("X_batch must be a list of strings for TfidfVectorizer")

        X_balanced, y_balanced = self.stratified_sampling(np.array(X_batch), np.array(y_batch))
        logging.info(f"Balanced batch size: {len(X_balanced)} malicious and {len(y_balanced)} benign samples.")

        X_transformed = self.vectorizer.fit_transform(X_balanced)
        logging.info(f"Transformed X_batch into feature matrix of shape {X_transformed.shape}")

        self.stacking_classifier.fit(X_transformed, y_balanced)

    def extract_features(self, X_batch):
        """Extract features using the vectorizer. Ensure X_batch is iterable."""
        if isinstance(X_batch, str):
            X_batch = [X_batch]
        return self.vectorizer.transform(X_batch).toarray()

    def classify(self, features):
        """Classify the features into labels."""
        return self.stacking_classifier.predict(features)

    def classify_proba(self, features):
        """Return the probability estimates for the classes."""
        return self.stacking_classifier.predict_proba(features)

    def process_single_url(self, url):
        """Process a single URL: extract features and classify."""
        features = self.extract_features([url])
        prediction = self.classify(features)
        return features, prediction

    def process_urls_in_parallel(self, url_list):
        """Process URLs in parallel using multiprocessing."""
        with Pool() as pool:
            results = pool.map(self.process_single_url, url_list)
        features_list, predictions = zip(*results)
        return features_list, predictions

    def calculate_metrics(self, y_true, y_pred, y_pred_proba=None):
        """Calculate various evaluation metrics."""
        
        accuracy = accuracy_score(y_true, y_pred)
        precision = precision_score(y_true, y_pred, average='binary')
        recall = recall_score(y_true, y_pred, average='binary')
        f1 = f1_score(y_true, y_pred, average='binary')
        
        if y_pred_proba is not None and len(y_pred_proba.shape) > 1:  # Ensure we have 2D probabilities
            roc_auc = roc_auc_score(y_true, y_pred_proba[:, 1])  # Use probability of positive class
        else:
            logging.warning("Predicted probabilities not available, skipping ROC AUC calculation.")
            roc_auc = None
        
        conf_matrix = confusion_matrix(y_true, y_pred)
    
        # Extract TP, TN, FP, FN from confusion matrix
        tn, fp, fn, tp = conf_matrix.ravel()
    
        # Calculate True Positive Rate (TPR) and False Positive Rate (FPR)
        tpr = tp / (tp + fn)  # True Positive Rate: Sensitivity/Recall
        fpr = fp / (fp + tn)  # False Positive Rate
    
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

    def evaluate_on_test_data(self, test_data):
        """Evaluate the model on test data and calculate all metrics."""
        y_true = test_data['label']
        url_list = test_data['url'].tolist()

        features_list, y_pred = self.process_urls_in_parallel(url_list)
        y_pred_proba = [self.classify_proba(features) for features in features_list]

        metrics = self.calculate_metrics(y_true, y_pred, y_pred_proba)
        return metrics

    def train_on_batch(self, X_batch, y_batch):
        """Alias for fit method to ensure compatibility."""
        self.fit(X_batch, y_batch)

    def save_model(self, path="models/ensemble_model.pkl"):
        """Save the trained model and vectorizer to disk."""
        directory = os.path.dirname(path)
        if not os.path.exists(directory):
            os.makedirs(directory)
        
        joblib.dump({'model': self.stacking_classifier, 'vectorizer': self.vectorizer}, path)
        logging.info(f"Model and vectorizer saved to {path}")

    def load_model(self, path="models/ensemble_model.pkl"):
        """Load the saved model and vectorizer from disk."""
        loaded_data = joblib.load(path)
        self.stacking_classifier = loaded_data['model']
        self.vectorizer = loaded_data['vectorizer']

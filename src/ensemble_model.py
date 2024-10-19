import numpy as np
from transformers import BertModel
from sklearn.linear_model import SGDClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from src.utils.model_helper import load_phishing_data
import logging

class EnsembleModel:
    def __init__(self):
        self.feature_extractor = BertModel.from_pretrained('bert-base-uncased')
        self.classifier = SGDClassifier(loss='log', warm_start=True)  # Online learning model
        self.logger = logging.getLogger('EnsembleModel')
    
    def extract_features(self, url):
        # Extract features using BERT
        features = self.feature_extractor(url)
        return features
    
    def train(self, urls, labels):
        # Convert URLs to BERT embeddings and train model
        features = np.array([self.extract_features(url) for url in urls])
        self.classifier.partial_fit(features, labels, classes=np.array([0, 1]))
    
    def classify(self, url):
        features = self.extract_features(url)
        return self.classifier.predict(features.reshape(1, -1))[0]

    def evaluate(self, true_labels, predicted_labels):
        accuracy = accuracy_score(true_labels, predicted_labels)
        precision = precision_score(true_labels, predicted_labels)
        recall = recall_score(true_labels, predicted_labels)
        f1 = f1_score(true_labels, predicted_labels)
        return accuracy, precision, recall, f1

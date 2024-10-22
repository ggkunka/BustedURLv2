import torch
from transformers import BertModel, RobertaModel, DistilBertModel, XLNetModel
from sklearn.ensemble import StackingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import (accuracy_score, precision_score, recall_score,
                             f1_score, roc_auc_score, confusion_matrix)
from sklearn.feature_extraction.text import TfidfVectorizer
from multiprocessing import Pool
from config.app_config import USE_XLNET  # Import the flag from configuration

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

    def extract_features(self, X_batch):
        """Extract features using the vectorizer."""
        return self.vectorizer.fit_transform(X_batch).toarray()

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

    def calculate_metrics(self, y_true, y_pred, y_pred_proba):
        """Calculate various evaluation metrics."""
        accuracy = accuracy_score(y_true, y_pred)
        precision = precision_score(y_true, y_pred, average='binary')
        recall = recall_score(y_true, y_pred, average='binary')
        f1 = f1_score(y_true, y_pred, average='binary')
        roc_auc = roc_auc_score(y_true, y_pred_proba[:, 1])  # Use probability of positive class
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

        # Parallel processing of URLs
        features_list, y_pred = self.process_urls_in_parallel(url_list)
        y_pred_proba = [self.classify_proba(features) for features in features_list]

        # Calculate metrics
        metrics = self.calculate_metrics(y_true, y_pred, y_pred_proba)
        return metrics

    def fit(self, X_batch, y_batch):
        """Train the model on a batch of data."""
        X_transformed = self.vectorizer.fit_transform(X_batch)
        self.stacking_classifier.fit(X_transformed, y_batch)

    def save_model(self, path="models/ensemble_model.pkl"):
        """Save the trained model to disk."""
        import joblib
        joblib.dump(self.stacking_classifier, path)

    def load_model(self, path="models/ensemble_model.pkl"):
        """Load a saved model from disk."""
        import joblib
        self.stacking_classifier = joblib.load(path)

from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

class RealTimeMetrics:
    def __init__(self):
        self.true_labels = []
        self.predicted_labels = []

    def update_metrics(self, true_label, predicted_label):
        self.true_labels.append(true_label)
        self.predicted_labels.append(predicted_label)

    def calculate_metrics(self):
        accuracy = accuracy_score(self.true_labels, self.predicted_labels)
        precision = precision_score(self.true_labels, self.predicted_labels)
        recall = recall_score(self.true_labels, self.predicted_labels)
        f1 = f1_score(self.true_labels, self.predicted_labels)
        return accuracy, precision, recall, f1

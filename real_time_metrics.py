import logging

def display_metrics_real_time(metrics):
    """Log and display real-time metrics."""
    logging.info("\n==== Real-Time Metrics ====")
    logging.info(f"Accuracy: {metrics['accuracy']:.2f}")
    logging.info(f"Precision: {metrics['precision']:.2f}")
    logging.info(f"Recall (TPR): {metrics['tpr']:.2f}")
    logging.info(f"F1 Score: {metrics['f1_score']:.2f}")
    logging.info(f"False Positive Rate (FPR): {metrics['fpr']:.2f}")
    logging.info(f"ROC AUC: {metrics['roc_auc']:.2f}")
    logging.info(f"TP: {metrics['tp']}, TN: {metrics['tn']}, FP: {metrics['fp']}, FN: {metrics['fn']}")

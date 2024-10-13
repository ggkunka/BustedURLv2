# BustedURL: Collaborative Multi-Agent System for Real-Time Malicious URL Detection

**BustedURL** is an open-source **Collaborative Multi-Agent System (CMAS)** designed for real-time detection of malicious URLs. The system leverages advanced deep learning models, including BERT, RoBERTa, DistilBERT, and optional XLNet, to accurately classify URLs as malicious or benign.

## Features:
- **Ensemble Learning**: Combines multiple transformer models for enhanced URL feature extraction.
- **Scalability**: Utilizes **Kafka** and **Celery** for distributed processing, making the system capable of handling millions of URLs efficiently.
- **Real-Time Detection**: CMAS architecture allows for real-time processing and classification with minimal latency.
- **Fault Tolerance**: Designed to handle agent failures gracefully, ensuring continuous operation in dynamic environments.
- **Extensible Architecture**: Easily extendable to incorporate additional agents or models for specialized tasks.

## Key Metrics:
- **Throughput**: Optimized for high throughput, processing large datasets in parallel.
- **Accuracy**: Achieves high true positive rates (TPR) with minimal false positives (FPR).
- **Resource Efficiency**: Efficiently utilizes CPU and memory resources across distributed agents.
- **Latency**: Minimizes latency for real-time classification tasks.

## Usage:
- The system is designed to be modular and customizable, making it suitable for research and deployment in large-scale cybersecurity infrastructures.
- Users can configure and run baseline tests or scalability tests, depending on their use case, with seamless parallelism and multi-agent coordination.

## License:
This project is licensed under the **GNU General Public License v3.0**. See the [LICENSE](./LICENSE) file for more details.

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

## Dir Structure:
BustedURL/
├── README.md                          # Update with new features for real-time ingestion and incremental learning
├── requirements.txt                   # Add dependencies for HDFS, Redis, and Kafka
├── config/
│   └── app_config.py                  # Configuration for batch size, HDFS paths, Redis, Kafka topics, model settings, etc.
├── data/
│   ├── cleaned_urls.csv               # Preprocessed data for initial training
│   └── real_time_test_urls.csv        # For real-time model evaluation
├── src/
│   ├── main.py                        # Entry point to start real-time ingestion, batch processing, and training
│   ├── ensemble_model.py              # Contains the ensemble learning model, supports incremental training
│   ├── cmas_agents.py                 # CMAS agents (DataCollectionAgent, FeatureExtractionAgent, etc.)
│   ├── batch_training.py              # New script for batch processing and training using HDFS data
│   └── utils/
│       ├── logger.py                  # Custom logger for system logs (already exists)
│       ├── model_helper.py            # Update to include incremental training, model saving, and loading
│       ├── hdfs_helper.py             # New: Helper functions to handle HDFS operations like data fetching
│       ├── redis_helper.py            # Helper functions to interact with Redis, manage URL cache
│       └── preprocessing.py           # New: Preprocessing functions for URLs (vectorization, embedding)
├── tests/
│   ├── test_scalability.py            # Testing scalability (update if needed for batch processing)
│   ├── test_baseline.py               # Test for baseline measurements with real-time ingestion
│   └── test_batch_training.py         # New: Tests for batch processing and incremental model training
├── run_scalability_test.py            # Script to run scalability tests
├── kafka_broker.py                    # Kafka broker (already exists, no major changes required)
├── celery_worker.py                   # Celery worker for task distribution (can remain the same)
├── real_time_data_ingestion.py        # Real-time data ingestion script (updated to support HDFS, Redis)
└── ids_ips/
    └── integration.py                 # IDS/IPS integration (extend as needed for threat intelligence updates)


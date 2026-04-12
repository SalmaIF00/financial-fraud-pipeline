Real-Time Financial Fraud Detection Pipeline 🛡️

An enterprise-grade Data Engineering solution designed for high-throughput ingestion, real-time processing, and anomaly detection in banking transactions. This project simulates a Swiss-standard financial environment, focusing on Data Governance, Scalability, and Real-time Observability.

🚀 System Architecture

The pipeline follows a modern distributed event-driven architecture:

Data Ingestion (Apache NiFi): Orchestrates the capture of transaction streams with built-in retry logic and backpressure management.

Streaming & Buffering (Apache Kafka): Acts as the backbone for real-time message distribution across the system.

Real-Time Processing (PySpark Streaming): Executes complex windowed analytics and rule-based fraud detection (e.g., velocity checks, high-value thresholds).

Data Quality & Contracts (Great Expectations): Automated validation layer that enforces "Data Contracts" before storage, ensuring zero-defect data ingestion.

Hybrid Storage Layer:

MongoDB: Optimized for "Hot Data" and immediate fraud alert retrieval.

MinIO (S3 Compatible): Serves as a Private Cloud Object Storage for raw landing zones, ensuring Swiss Data Sovereignty compliance.

Hadoop (HDFS): Long-term Cold Storage for historical auditing and ML model retraining.

Observability (Prometheus & Grafana): Live monitoring of transaction throughput, system health, and fraud KPI dashboards.

🛠️ Tech Stack (Swiss Market Focus)

Streaming: Apache Kafka, Apache NiFi

Big Data Engine: PySpark (Spark Streaming)

Quality Assurance: Great Expectations

Infrastructure: Docker, Docker Compose, MinIO

Monitoring: Grafana, Prometheus

Languages: Python (SQL, Pandas, PySpark)

📊 Business Logic & Fraud Patterns

The system implements multiple detection layers:

Anti-Money Laundering (AML) Compliance: Automated flagging of transactions exceeding 5,000 CHF/EUR.

Velocity Attack Detection: Identifies more than 3 transactions from the same source within a 60-second window using Spark Window Functions.

Geographic "Impossible Travel": Flags consecutive transactions from distant locations in an impossible timeframe.

Automated Quarantine: Any data failing the Great Expectations quality gate is automatically routed to a dedicated quarantine bucket in MinIO.

📂 Project Structure

/financial-fraud-pipeline
├── /infrastructure       # Docker Compose, Prometheus config, Grafana dashboards
├── /nifi-templates       # XML flow definitions with error handling
├── /pyspark-streaming    # Real-time processing logic and Spark jobs
├── /data-validation      # Great Expectations suites and data contracts
├── /automation           # GitHub Actions for CI/CD and automated testing
└── README.md             # Main documentation


Developed as a high-impact technical capstone project to demonstrate proficiency in building secure, automated, and scalable financial data ecosystems.

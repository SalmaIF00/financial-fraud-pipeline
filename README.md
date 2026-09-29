# Real-Time Financial Fraud Detection Pipeline 🛡️

An enterprise-grade Data Engineering solution designed for high-throughput ingestion, real-time distributed stream processing, and anomaly detection in banking transactions. This project simulates a Swiss-standard financial environment, emphasizing strict Data Governance, Scalability, and Real-Time Observability.

---

## 🚀 System Architecture

The pipeline follows a modern distributed event-driven architecture built for low latency, fault tolerance, and strict data reliability:

[Transaction Generator]
│
▼
[Apache Kafka] ─── (Topic: financial-transactions)
│
▼
[PySpark Streaming Engine]
├── Anomaly & Fraud Detection Rules
├── Windowed Aggregations (Velocity Checks)
└── Metrics Extraction
│
├──► [Alerts / High-Risk Storage] (Hot Layer)
└──► [Prometheus Exporter] ──► [Prometheus] ──► [Grafana Dashboards]


* **Data Simulation & Ingestion:** Synthetic transaction streams generated via `scripts/transaction_generator.py` simulating standard ISO financial payloads with randomized legitimate and fraudulent behavioral patterns.
* **Streaming & Buffering (Apache Kafka):** Serves as the distributed message broker, decoupling producers from consumers with consumer group horizontal scaling.
* **Real-Time Processing (PySpark Structured Streaming):** Consumes streaming records from Kafka, runs stateless parsing and stateful windowed aggregations, and evaluates fraud criteria on the fly.
* **Observability & Metrics (Prometheus & Grafana):** Monitors transaction throughput, pipeline latency, compute memory usage, and fraud detection rates in real time.

---

## 🛠️ Tech Stack

* **Streaming & Ingestion:** Python (`kafka-python` / `faker`), Apache Kafka
* **Stream Processing Engine:** Apache Spark (PySpark Structured Streaming)
* **Infrastructure & Containerization:** Docker, Docker Compose
* **Observability:** Prometheus, Grafana
* **Languages:** Python 3.10+, SQL

---

## 📊 Business Logic & Fraud Patterns

The engine evaluates incoming transactions against configurable fraud heuristics:

1. **High-Value Thresholds (AML Compliance):** Flags single transactions exceeding defined limits (e.g., > 5,000 CHF/EUR) for compliance review.
2. **Velocity Attacks:** Flags multiple rapid transactions originating from the same account ID within a tumbling or sliding time window (e.g., > 3 transactions within 60 seconds).
3. **Location Impossibility / Geolocation Drift:** Identifies concurrent or near-instant transactions on the same card executed across geographically incompatible regions.
4. **Suspicious Merchant / Terminal Categories:** Real-time flagging of known high-risk Merchant Category Codes (MCC).

---

## 📂 Project Structure


financial-fraud-pipeline/
├── infrastructure/
│   ├── docker-compose.yml          # Container orchestration (Kafka, Spark, Prometheus)
│   └── prometheus.yml              # Prometheus scraping jobs and targets
├── pyspark-streaming/
│   └── fraud_detector.py           # Real-time PySpark streaming and detection logic
├── scripts/
│   └── transaction_generator.py    # Synthetic banking transaction producer
├── .gitignore                      # Git exclusion rules
├── requirements.txt                # Python runtime dependencies
└── README.md                       # Project documentation
⚡ Quickstart & Deployment
1. Prerequisites
Docker Engine (>= 24.0) & Docker Compose (>= 2.20)

Python 3.10+

Git

2. Environment Setup
Clone the repository and set up a local virtual environment:

Bash
git clone [https://github.com/TU_USUARIO/financial-fraud-pipeline.git](https://github.com/SalmaIF00/financial-fraud-pipeline.git)
cd financial-fraud-pipeline
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
3. Spin Up Infrastructure
Launch the Kafka cluster, Spark master/worker nodes, and Prometheus via Docker Compose:

Bash
docker compose -f infrastructure/docker-compose.yml up -d
Verify that all services are operational:

Bash
docker compose -f infrastructure/docker-compose.yml ps
4. Run PySpark Streaming Job
Submit or execute the fraud detection streaming application:

Bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  pyspark-streaming/fraud_detector.py
5. Start Transaction Producer
In a separate terminal, launch the transaction generator to feed events into Kafka:

Bash
python scripts/transaction_generator.py
6. Observability
Prometheus UI: http://localhost:9090

Spark UI: http://localhost:4040 (or 8080 for Standalone Master)

Kafka Broker: localhost:9092

🔒 Security & Data Governance
Designed with zero plain-text credential persistence; uses environment-injected secrets.

Structured to allow pluggable Data Quality validation (e.g., Great Expectations / Soda Core) before final analytical sink insertion.

Built strictly adhering to standard corporate Git workflows with committed branch protection and pipeline modularity.

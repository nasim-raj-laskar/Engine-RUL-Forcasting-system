<div align="center">

# ✈️ Aircraft Engine Remaining Useful Life (RUL) Forecasting Platform

*Real-time predictive maintenance, distributed telemetry streaming, and Bayesian uncertainty quantification on NASA C-MAPSS turbofan engines.*

[![Python](https://img.shields.io/badge/Python-3.12-3776AB.svg?logo=python&logoColor=white)](https://www.python.org/)
[![TensorFlow](https://img.shields.io/badge/TensorFlow-2.17-FF6F00.svg?logo=tensorflow&logoColor=white)](https://www.tensorflow.org/)
[![Apache Flink](https://img.shields.io/badge/Apache_Flink-2.0-E6526F.svg?logo=apacheflink&logoColor=white)](https://flink.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-3.7-231F20.svg?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Solace](https://img.shields.io/badge/Solace-PubSub+-00C896.svg)](https://solace.com/)
[![Redis](https://img.shields.io/badge/Redis-7.2_Feature_Store-DC382D.svg?logo=redis&logoColor=white)](https://redis.io/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-009688.svg?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![Vue.js](https://img.shields.io/badge/Vue.js-3.5-4FC08D.svg?logo=vuedotjs&logoColor=white)](https://vuejs.org/)
[![MLflow](https://img.shields.io/badge/MLflow-Tracking_&_Registry-0194E2.svg?logo=mlflow&logoColor=white)](https://mlflow.org/)
[![Prometheus](https://img.shields.io/badge/Prometheus-Monitoring-E6522C.svg?logo=prometheus&logoColor=white)](https://prometheus.io/)
[![Grafana](https://img.shields.io/badge/Grafana-15+_Panels-F46800.svg?logo=grafana&logoColor=white)](https://grafana.com/)
[![Docker](https://img.shields.io/badge/Docker-Compose_Stack-2496ED.svg?logo=docker&logoColor=white)](https://www.docker.com/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

<br/>

**Fleet Size:** `100 Concurrent Turbofans` &nbsp;•&nbsp; **Stream Processing:** `Apache Flink 2.0 RocksDB` &nbsp;•&nbsp; **Test RMSE:** `14.99 Cycles` &nbsp;•&nbsp; **Feature Latency:** `Sub-Millisecond`

</div>

---

<h2 align="center">🏗️ High-Level Architecture</h2>

<p align="center">
  <img src="assets\architecture.png" alt="High-Level Architecture Diagram" width="100%" />
</p>

---

<h2 align="center">⚡ Core Engineering Highlights</h2>

* **Distributed Ingestion**: Dedicated event broker (Solace PubSub+ SMF protocol) bridges high-frequency telemetry into an Apache Kafka partitioned event log via Kafka Connect.
* **Stateful Stream Processing**: Apache Flink 2.0 maintains keyed RocksDB operator state with tumbling/sliding 30-cycle temporal windows and 60-second exactly-once checkpointing.
* **Dual-Sink Storage Fabric**: Sub-millisecond online feature lookup via Redis Feature Store (`float32[330]` tensors with 1-hour TTL) alongside long-term Hive-partitioned S3 Parquet sinks.
* **Deep Sequence Model**: 3-layer Gated Recurrent Unit (GRU 128 → 64 → 32) with spatial dropout regularization, dense projection head, and normalized target formulation.
* **Bayesian Uncertainty**: Monte Carlo Dropout (30 stochastic forward passes at inference) yields empirical variance for epistemic uncertainty quantification and confidence calibration.
* **Vectorized Fleet Inference**: FastAPI batches fleet-wide feature tensors into a single forward pass ($O(1)$ model invocations) broadcast via WebSockets on a 5-second tick loop.
* **Live Observability**: Prometheus scraping with 9 custom gauges + auto-provisioned 15-panel Grafana dashboard + Evidently AI 0.7 KS-test drift reports embedded in the UI.

---

<h2 align="center">📊 Validated Model Benchmarks (NASA C-MAPSS FD001)</h2>

Evaluated against the 100-engine test set with piece-wise linear target clipping ($RUL_{\text{max}} = 125$ cycles):

<div align="center">

| Metric | Measured Value | Target Gate  |
| :--- | :---: | :---: |
| **Root Mean Squared Error (RMSE)** | **14.99 cycles** | $< 20.0$ cycles|
| **NASA Asymmetric Penalty Score** | **449.6** | $< 2000.0$  |
| **Critical Regime Precision** | **91.7%** | $> 80.0\%$ |
| **Critical Regime Recall** | **88.0%** | $> 75.0\%$  |
| **Critical F1 Score** | **0.898** | $> 0.80$ |
| **Classification Accuracy** | **95.0%** | $> 80.0\%$ |
| **Weighted F1 Score** | **0.950** | $> 0.80$  |

</div>

> ℹ️ *Mathematical formulations for the piecewise degradation targets, critical-zone sample weights, and the NASA asymmetric penalty function are detailed in [docs/04_model_training.md](docs/04_model_training.md).*

---

<h2 align="center">🖥️ Operations & Observability</h2>

<h3 align="center">🎮 Fleet Command Center (<code>/</code>)</h3>
<p align="center">
  <em>Real-time operations cockpit displaying active fleet health, risk classification distributions, urgent alerts, and telemetry feeds.</em>
</p>
<p align="center">
  <img src="assets/fleetpage.png" alt="Fleet Command Center" width="95%" />
</p>

<br/>

<h3 align="center">📈 Observability Suite (<code>:3000</code>)</h3>
<p align="center">
  <em>Infrastructure & model metrics: prediction throughput, latency quantiles (p50/p95/p99), critical engine counts, and Redis memory telemetry.</em>
</p>
<p align="center">
  <img src="assets/grafana.png" alt="Grafana Dashboard" width="95%" />
</p>

---

<h2 align="center">🚀 Quick Start</h2>

### 1. Launch Core Stack (FastAPI + Redis + Producer + Consumer + Dashboard)
```bash
git clone https://github.com/nasim-raj-laskar/Real-Time-Aircraft-Engine-Predictive-Maintenance-System.git
cd Real-Time-Aircraft-Engine-Predictive-Maintenance-System

cp .env.example .env
cp solace.env.example streaming/config/solace.env
# Fill in AWS, MLflow & broker credentials

# Core services (~2.4GB RAM)
docker compose up -d
```

### 2. Enable Distributed Streaming & Monitoring Profiles
```bash
# Add Kafka, Flink, and Solace
docker compose --profile streaming up -d

# Add Prometheus, Grafana, and Exporters
docker compose --profile monitoring up -d

# Launch all 13 services together
docker compose --profile streaming --profile monitoring up -d
```

### 3. Service Access Matrix

<div align="center">

| Service | Endpoint | Access Credentials | Compose Profile |
| :--- | :--- | :---: | :---: |
| **Fleet Operations Dashboard** | `http://localhost:5173` | Public | `core` |
| **Inference API (Swagger)** | `http://localhost:8000/docs` | Public | `core` |
| **Grafana Monitoring Suite** | `http://localhost:3000` | `admin` / `admin` | `monitoring` |
| **Prometheus Raw Metrics** | `http://localhost:9090` | Public | `monitoring` |
| **Apache Flink Web UI** | `http://localhost:8082` | Public | `streaming` |
| **Solace Broker Management** | `http://localhost:8080` | `admin` / `admin` | `streaming` |
| **Kafka Connect REST API** | `http://localhost:8083` | Public | `streaming` |

</div>

---

<h2 align="center">📖 Documentation & Architectural Deep Dives</h2>

<details open>
<summary><b>Click to expand / collapse module specifications</b></summary>
<br>

| Module | Document | Description |
| :---: | :--- | :--- |
| **00** | [Documentation Index](docs/00_index.md) | Global system navigation, architectural topic map, and parameter catalog |
| **01** | [Dataset Reference](docs/01_dataset.md) | C-MAPSS FD001 sensor physics, variance filtering, and degradation curves |
| **02** | [Preprocessing Pipeline](docs/02_preprocessing.md) | Deterministic sensor dropping, piecewise linear RUL target math, MinMaxScaler |
| **03** | [Feature Engineering](docs/03_feature_engineering.md) | Sliding window geometry ($30 \times 11$), target scaling, and temporal structures |
| **04** | [Model Training & Registry](docs/04_model_training.md) | 3-layer GRU topology, MC Dropout math, NASA scoring formulas, and MLflow gates |
| **05** | [Inference Service](docs/05_inference_service.md) | FastAPI asynchronous runtime, 5s vectorized batch prediction loop, REST/WS specs |
| **06** | [Streaming Pipeline](docs/06_streaming_pipeline.md) | Solace SMF, Kafka KRaft log, PyFlink 2.0 RocksDB exactly-once windowing |
| **07** | [Monitoring & Observability](docs/07_monitoring.md) | Prometheus metrics matrix, alert rules, and Evidently AI 0.7 KS-drift pipeline |
| **07.1**| [Operations Dashboard UI](docs/07.1_UI.md) | Vue 3 SPA architecture, Pinia reactive state stores, and WebSocket clients |
| **08** | [Project Structure](docs/08_project_structure.md) | Codebase directory tree, 7-stage ML pipeline, and container resource limits |
| **09** | [System Architecture](docs/09_architecture.md) | Multi-tier architecture diagrams, message lifecycle sequences, and network mesh |

</details>

---

<h2 align="center">📄 License</h2>

<div align="center">

Released under the [MIT License](LICENSE). Built for high-reliability predictive maintenance research and deployments.

</div>

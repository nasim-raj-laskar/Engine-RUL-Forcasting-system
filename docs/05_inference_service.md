# Inference Service Specification

## Asynchronous Runtime, Vectorized WebSocket Loops & Observability

The inference service is built on FastAPI and Uvicorn, exposing high-throughput REST endpoints, Server-Sent Events (SSE) for pipeline retraining streaming, and low-latency WebSocket channels for fleet-wide continuous predictions.

---

## ⚡ Multi-Pathway Request Lifecycle

```mermaid
sequenceDiagram
    autonumber
    participant Client as Client Application
    participant API as FastAPI (:8000)
    participant Redis as Redis Feature Store
    participant Engine as TensorFlow GRU Engine

    rect rgb(240, 249, 255)
    Note over Client,Engine: Online Stream Pathway (Sub-millisecond)
    Client->>API: GET /predict/engine/{id}
    API->>Redis: GET engine:{id}:features (330 float32)
    Redis-->>API: Binary Feature Tensor
    API->>Engine: MC Dropout Inference (30 Passes)
    Engine-->>API: Mean RUL + Epistemic Uncertainty
    API-->>Client: {rul, failure_risk, risk_level, confidence}
    end

    rect rgb(254, 242, 242)
    Note over Client,Engine: Direct Sensor Payload Pathway
    Client->>API: POST /predict/raw {raw_sensor_dict}
    API->>API: InferencePreprocessor (Global MinMax Transform)
    API->>Engine: Forward Pass
    Engine-->>API: Inference Output
    API-->>Client: {rul, failure_risk, risk_level, confidence}
    end
```

---

## 🔄 Vectorized Batch Prediction Loop (`/ws/predictions`)

To eliminate the $O(N)$ computational bottleneck of scoring 100 individual engines sequentially, the background prediction task executes a single vectorized forward pass every 5.0 seconds:

```mermaid
flowchart TD
    TICK[5-Second Interval Timer] --> DISCOVER[Query Active Fleet Keys\nKEYS engine:*:features]
    DISCOVER --> FETCH[Pipeline MGET Feature Tensors\n330 float32 values per engine]
    FETCH --> STACK[Vectorize into Single NumPy Tensor\nShape: N × 30 × 11]
    STACK --> FORWARD[Single Batched Model Execution\nTensorFlow / Keras Forward Pass]
    FORWARD --> GAUGES[Update Prometheus Metrics\ncritical_engines_total · latency · requests]
    FORWARD --> BROADCAST[Broadcast JSON Fleet Payload\nWebSocket Broadcast to Connected UIs]

    style FORWARD fill:#1e293b,stroke:#0ea5e9,color:#fff
    style BROADCAST fill:#1e293b,stroke:#22c55e,color:#fff
```

---

## 🔌 API Endpoint Catalog

| Method | Route | Description | Primary Consumers |
| :--- | :--- | :--- | :--- |
| `POST` | `/predict` | Predict from normalized $30 \times 11$ matrix | External ML services |
| `POST` | `/predict/raw` | Transform and predict from unscaled sensor history | Direct IoT integrations |
| `GET`  | `/predict/engine/{id}` | High-speed inference using Redis online features | Dashboard engine detail |
| `GET`  | `/predict/stream/{id}` | Inference against the in-memory push buffer | Simulation & replay lab |
| `POST` | `/push` | Ingest single reading into engine circular buffer | Synthetic replay agents |
| `GET`  | `/engines` | Catalog active engines and health snapshots | Operations overview |
| `GET`  | `/alerts` | Query active HIGH and CRITICAL risk engines | Operational alert systems |
| `GET`  | `/health` | Liveness probe returning component uptimes | Container orchestrators |
| `GET`  | `/model/info` | Return model hyperparameters, input tensor geometry | MLOps dashboards |
| `GET`  | `/model/evaluation` | Real-time metrics read from `metrics.json` | MLOps dashboards |
| `GET`  | `/metrics` | Prometheus exposition format telemetry | Prometheus server |
| `POST` | `/pipeline/run` | Spawn asynchronous non-blocking retraining run | MLOps UI console |
| `GET`  | `/pipeline/status` | Current training state (`idle`, `running`, `success`) | MLOps UI polling |
| `GET`  | `/pipeline/logs` | Server-Sent Events (SSE) execution log stream | Live terminal panels |
| `GET`  | `/drift/reports` | Index generated Evidently AI 0.7 HTML reports | MLOps report viewer |
| `GET`  | `/drift/reports/{file}` | Serve interactive standalone Evidently HTML | In-dashboard iframe |
| `WS`   | `/ws/predictions` | Real-time fleet predictions (5s tick interval) | Fleet overview charts |
| `WS`   | `/ws/telemetry` | Raw cycle and sensor telemetry feed (2s tick) | Telemetry log tables |
| `WS`   | `/ws/alerts` | Urgent fleet health exceptions (5s tick) | Real-time warning banners |

---

## 🗄️ Online Feature Store Architecture (Redis)

| Key Pattern | Data Structure | Payload Specification | TTL Policy |
| :--- | :--- | :--- | :--- |
| `engine:{id}:features` | Binary String | 330 Big-Endian `float32` values ($30 \times 11 = 1320$ bytes) | **3600s** (Sliding refresh) |
| `engine:{id}:meta` | Redis Hash | Fields: `engine_id`, `cycle`, `event_time`, `window_size` | **3600s** (Sliding refresh) |
| `engine:{id}:buffer` | Redis List | JSON string entries containing raw sensor frames | **3600s** (Sliding refresh) |

> **Graceful Failure Signal**: When physical telemetry halts for an engine, the Redis key expires after 1 hour. Subsequent calls return `404 Not Found`, automatically preventing the model from inferring against stale data.

---

## 📊 Prometheus Telemetry Instrumentation

```mermaid
flowchart LR
    API[FastAPI Runtime] -->|Export on :8000/metrics| PROM[Prometheus Scraper]
    PROM -->|Query Evaluator| GRAF[Grafana Visualizer]
    PROM -->|Rule Evaluator| ALERT[Alertmanager Triggers]

    style API fill:#1e293b,stroke:#0ea5e9,color:#fff
    style GRAF fill:#1e293b,stroke:#f97316,color:#fff
```

| Metric Identifier | Metric Type | Target Monitored |
| :--- | :--- | :--- |
| `active_engines_total` | **Gauge** | Fleet size currently populated in Redis Feature Store |
| `critical_engines_total` | **Gauge** | Snapshot count of engines in CRITICAL failure regime |
| `prediction_requests_total` | **Counter** | Total predictions served, labeled by `risk_level` |
| `prediction_latency_seconds` | **Histogram** | Execution duration of the batched model forward pass |
| `predicted_rul_cycles` | **Histogram** | Statistical distribution of predicted RUL outputs |
| `failure_risk_score` | **Histogram** | Statistical distribution of normalized risk scores |
| `prediction_confidence` | **Histogram** | Distribution of Bayesian epistemic confidence bounds |
| `prediction_errors_total` | **Counter** | Error count partitioned by exception class |
| `model_load_time_seconds` | **Gauge** | Artifact deserialization latency during cold start |

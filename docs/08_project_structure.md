# Project Structure & Microservice Topology

## Codebase Organization, MLOps Stages & Container Orchestration

A modular monorepo cleanly separating ML lifecycle pipelines, streaming workers, asynchronous inference runtimes, and frontend presentation layers.

---

## 🗂️ Monorepo Architecture

```
Real-Time-Aircraft-Engine-Predictive-Maintenance-System/
├── Dataset/                 # C-MAPSS FD001 raw reference telemetry
├── config/                  # Pipeline, model, and infrastructure YAML definitions
│   ├── config.yaml          # S3 paths, artifact storage schemas
│   ├── features.yaml        # Sensor selection, window geometry (30x11)
│   ├── model.yaml           # 3-layer GRU hyperparameters & regularization
│   ├── params.yaml          # Adam optimizer learning rate, batch size, epochs
│   ├── redis.yaml           # Feature store host, connection pool, TTL
│   ├── registor.yaml        # MLflow quality gate thresholds
│   └── schema.yaml          # Telemetry column validation constraints
├── src/                     # Core Python ML and backend microservices
│   ├── components/          # 7 pipeline stage implementations
│   ├── pipeline/            # Orchestrators executing each stage
│   ├── inference/           # FastAPI application, WebSockets, feature store client
│   ├── monitoring/          # Evidently AI 0.7 KS-drift detector
│   ├── cloud/               # AWS S3 client wrapper
│   └── metrics/             # Custom RMSE and NASA asymmetric scoring logic
├── streaming/               # Real-time distributed stream processing
│   ├── producer/            # Risk-distributed 100-engine fleet simulator
│   ├── pipeline/            # PyFlink 2.0 streaming pipeline & operators
│   │   ├── functions/       # Stateless normalization & RocksDB windowing
│   │   └── sinks/           # Redis feature store & S3 Parquet sinks
│   └── model/               # Event & FeatureVector data serialization
├── frontend/                # Vue 3 + Vite + TypeScript operations dashboard
│   └── src/
│       ├── pages/           # 5 operational SPA views
│       ├── components/      # ECharts widgets, SVG network diagrams
│       ├── stores/          # Pinia reactive state stores
│       └── composables/     # Multi-channel WebSocket management
├── monitoring/              # Infrastructure observability definitions
│   ├── prometheus/          # Scrape targets & alerting rule YAMLs
│   └── grafana/             # Auto-provisioned 15-panel JSON dashboards
├── reports/drift/           # Persisted Evidently AI 0.7 HTML drift reports
├── artifacts/               # Generated pipeline binaries, scalers, and metrics
└── docker-compose.yml       # 13-service microservice orchestration
```

---

## 🔄 7-Stage Pipeline Lifecycle

```mermaid
flowchart LR
    S1["1. Ingestion\nS3 Bronze → Local"] --> S2["2. Validation\nSchema & Null Check"]
    S2 --> S3["3. Transformation\nParquet + Scaler"]
    S3 --> S4["4. Features\n30×11 Sequences"]
    S4 --> S5["5. Training\n3-Layer GRU"]
    S5 --> S6["6. Evaluation\nRMSE · NASA · F1"]
    S6 --> S7["7. Registry\nQuality Gate & S3"]

    style S1 fill:#1e293b,stroke:#0ea5e9,color:#fff
    style S2 fill:#1e293b,stroke:#0ea5e9,color:#fff
    style S3 fill:#1e293b,stroke:#0ea5e9,color:#fff
    style S4 fill:#1e293b,stroke:#0ea5e9,color:#fff
    style S5 fill:#1e293b,stroke:#0ea5e9,color:#fff
    style S6 fill:#1e293b,stroke:#0ea5e9,color:#fff
    style S7 fill:#1e293b,stroke:#22c55e,color:#fff
```

---

## 🐳 Containerized Service Topology

```mermaid
graph TB
    subgraph Messaging["Event Fabric"]
        SOL["aircraft-solace\nSMF :55555 :8080"]
        KC["aircraft-kafka-connect\nConnector :8083"]
        KF["aircraft-kafka\nKRaft :9092 :29092"]
    end

    subgraph Streaming["Stream Processing"]
        PROD["aircraft-producer\n100-Engine Fleet"]
        JM["aircraft-flink-jobmanager\nWeb UI :8082"]
        TM["aircraft-flink-taskmanager\n3 Task Slots"]
    end

    subgraph Storage["Online & Offline State"]
        RD["aircraft-redis\nFeature Store :6379"]
    end

    subgraph Application["Inference & UI"]
        API["aircraft-engine-api\nFastAPI :8000"]
        FE["aircraft-frontend\nVue 3 + Nginx :5173"]
    end

    subgraph Observability["Monitoring"]
        PROM["aircraft-prometheus\n:9090"]
        GRAF["aircraft-grafana\n:3000"]
    end

    PROD --> SOL --> KC --> KF --> TM
    JM --- TM
    TM --> RD
    RD --> API
    API --> FE
    API & RD --> PROM --> GRAF

    style SOL fill:#4a1d96,color:#fff
    style KF fill:#f59e0b,color:#000
    style TM fill:#0369a1,color:#fff
    style RD fill:#991b1b,color:#fff
    style API fill:#15803d,color:#fff
    style GRAF fill:#ea580c,color:#fff
```

---

## ⚙️ Microservice Resource Allocation

| Container Service | Base Image | Memory Limit | CPU Limit | Primary Responsibility |
| :--- | :--- | :--- | :--- | :--- |
| `aircraft-engine-api` | `python:3.12-slim` | **1.5 GB** | 2.0 cores | FastAPI, TensorFlow runtime, model inference |
| `aircraft-frontend` | `nginx:alpine` | **256 MB** | 0.5 cores | Static SPA hosting & API reverse proxy |
| `aircraft-redis` | `redis:7.2-alpine` | **256 MB** | 1.0 cores | Sub-millisecond online feature store |
| `aircraft-solace` | `solace-pubsub-standard` | **1.5 GB** | 2.0 cores | Ingestion SMF message broker |
| `aircraft-kafka` | `cp-kafka:7.6.0` | **1.0 GB** | 1.5 cores | Durable partitioned telemetry log |
| `aircraft-kafka-connect` | `cp-kafka-connect:7.6.0` | **768 MB** | 1.0 cores | Solace-to-Kafka automated source connector |
| `aircraft-flink-jobmanager` | `flink:2.0-scala_2.12` | **1.0 GB** | 1.0 cores | Flink cluster coordination & Web UI |
| `aircraft-flink-taskmanager` | `Dockerfile.streaming` | **1.5 GB** | 2.0 cores | PyFlink operator execution & RocksDB |
| `aircraft-producer` | `Dockerfile.streaming` | **256 MB** | 0.5 cores | 100-engine fleet telemetry simulation |
| `aircraft-prometheus` | `prometheus:v2.50.0` | **256 MB** | 0.5 cores | Metric collection & rule evaluation |
| `aircraft-grafana` | `grafana:10.3.0` | **256 MB** | 0.5 cores | Production visual analytics dashboard |
| `aircraft-node-exporter` | `node-exporter:v1.7.0` | **128 MB** | 0.2 cores | Host hardware & OS metrics |
| `aircraft-redis-exporter` | `redis_exporter:v1.58.0` | **128 MB** | 0.2 cores | Redis memory, command, & key metrics |

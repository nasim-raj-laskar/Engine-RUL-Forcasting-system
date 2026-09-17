# System Architecture Specification

## Multi-Tier Architecture, Real-Time Ingestion Sequences & Network Topology

An end-to-end specification of data flow across the ingestion fabric, stream windowing engine, online feature store, asynchronous inference runtime, and monitoring subsystem.

---

## 🏛️ Multi-Tier Architecture Topology

```mermaid
flowchart TB
    subgraph Data["Data Layer (AWS S3 Medallion)"]
        A[NASA C-MAPSS FD001] --> B[S3 Bronze\nRaw Reference Files]
        B --> C[S3 Silver\nCleaned Parquet + Scaler]
        C --> D[S3 Gold\nNumPy 30×11 Sequences]
    end

    subgraph Pipeline["ML Pipeline (7 Stages)"]
        D --> E[GRU Training\n3-Layer 128→64→32]
        E --> F[Evaluation Engine\nRMSE · NASA · F1 · Residuals]
        F --> G{Quality Gate Check\nRMSE < 20.0 · NASA < 2000.0}
        G -->|Pass| H[MLflow Model Registry\nDagsHub Hosted Tracking]
        G -->|Fail| E
        H --> I[S3 Production Artifacts\nmodel.keras · scaler.pkl]
    end

    subgraph Stream["Distributed Streaming Pipeline"]
        J[Telemetry Producer\n100 Engines · Risk-Distributed] -->|SMF Publish :55555| SOL[Solace PubSub+ Broker\nSMF Wildcard Routing]
        SOL --> KC[Solace Kafka Connector\nManaged Bridge :8083]
        KC -->|Produce Partitioned| KF[Apache Kafka Log\ntelemetry.raw · 3 Partitions]
        KF -->|KafkaSource| FL[PyFlink 2.0 Engine\nExactly-Once Checkpoints · RocksDB]
        FL --> M[Redis Feature Store\nengine:id:features · TTL 1h]
        FL --> N[S3 Parquet Sink\nHive-Partitioned date/hour]
    end

    subgraph Infer["Inference Subsystem"]
        I --> O[FastAPI Engine :8000\nVectorized TF Batch Forward Pass]
        M --> O
        O --> P[REST + WebSocket + SSE]
    end

    subgraph UI["Operations Console"]
        P --> Q[Vue 3 Dashboard\n5 Pages · 3 WS Streams]
    end

    subgraph Mon["Observability Suite"]
        P --> R[Prometheus Engine :9090]
        R --> S[Grafana Enterprise :3000\n15+ Production Panels]
        T[Evidently AI 0.7 Engine\nKS-Test Drift Detector] --> U[Interactive HTML Reports\nServed via API]
        U --> Q
    end

    style G fill:#eab308,stroke:#333,stroke-width:2px,color:#000
    style H fill:#22c55e,stroke:#333,stroke-width:2px,color:#fff
    style O fill:#0ea5e9,stroke:#333,stroke-width:2px,color:#fff
    style Q fill:#a855f7,stroke:#333,stroke-width:2px,color:#fff
    style FL fill:#0284c7,stroke:#333,stroke-width:2px,color:#fff
    style KF fill:#f59e0b,stroke:#333,stroke-width:2px,color:#000
    style SOL fill:#6d28d9,stroke:#333,stroke-width:2px,color:#fff
```

---

## ⚡ End-to-End Lifecycle Sequence

```mermaid
sequenceDiagram
    autonumber
    participant PROD as Telemetry Producer (100 Engines)
    participant SOL as Solace PubSub+ Broker
    participant KC as Solace Kafka Connector
    participant KF as Apache Kafka (telemetry.raw)
    participant FL as PyFlink 2.0 Job
    participant R as Redis Feature Store
    participant S3 as Amazon S3 Parquet
    participant API as FastAPI Inference Worker
    participant WS as Vue 3 Dashboard

    PROD->>SOL: SMF binary publish (aircraft/engine/{id}/telemetry/cycle)
    SOL->>KC: Ingest message into bound queue
    KC->>KF: Write record with engine_id partition key
    KF->>FL: KafkaSource consumer group read
    FL->>FL: Stateless MinMax normalizer
    FL->>FL: RollingWindowProcess (RocksDB keyed state)
    FL->>R: Atomic MSET engine:{id}:features (330 float32 bytes)
    FL->>S3: Flush Snappy Parquet on checkpoint completion (60s)

    Note over API: Batched execution loop every 5.0 seconds
    API->>R: KEYS engine:*:features & MGET tensors
    API->>API: Stack into (N, 30, 11) & batched forward pass
    API->>WS: Broadcast predictions & alert states over WebSockets
```

---

## 🌊 Streaming Windowing Mechanics

```mermaid
flowchart LR
    subgraph StreamInput["Kafka Partition Ingest"]
        RAW["telemetry.raw Record\nKey: engine_id · Value: 11 Sensors"]
    end

    subgraph FlinkTopology["PyFlink TaskManager Runtime"]
        MAP["NormalizeMap\nClamp to 0.0 to 1.0"]
        KEY["keyBy(engine_id)\nThread Affinity"]
        PROC["RollingWindowProcess\n30-Cycle ListState Buffer"]
        CHECK{"Window\nSize == 30?"}
        EMIT["Emit FeatureVector\nDrop Oldest Cycle"]
        HOLD["Accumulate in State\nAwait Next Cycle"]
    end

    subgraph Sinks["Storage Targets"]
        REDIS["Redis Online Store\nTTL 3600s"]
        PARQ["S3 Offline Store\nHive Parquet"]
    end

    RAW --> MAP --> KEY --> PROC --> CHECK
    CHECK -->|Yes| EMIT
    CHECK -->|No| HOLD
    EMIT --> REDIS & PARQ

    style PROC fill:#0369a1,color:#fff
    style REDIS fill:#991b1b,color:#fff
    style PARQ fill:#15803d,color:#fff
```

![Streaming Operations Console](../assets/pipelinepage.png)

---

## 🖥️ Containerized Infrastructure Mesh

```mermaid
graph TB
    subgraph Edge["Ingestion Fabric"]
        SOL["aircraft-solace\nSMF :55555 · REST :8080"]
        KC["aircraft-kafka-connect\nREST :8083"]
        KF["aircraft-kafka\nKRaft :9092 · Ext :29092"]
    end

    subgraph FlinkCluster["Stream Processing"]
        JM["aircraft-flink-jobmanager\nDashboard :8082"]
        TM["aircraft-flink-taskmanager\n3 Task Slots"]
        PROD["aircraft-producer\n100-Engine Fleet"]
    end

    subgraph StorageMesh["Storage"]
        RD["aircraft-redis\nFeature Store :6379"]
    end

    subgraph CoreApplication["Inference & UI"]
        API["aircraft-engine-api\nFastAPI :8000"]
        FE["aircraft-frontend\nVue 3 SPA :5173"]
    end

    subgraph MonitoringStack["Observability"]
        PROM["aircraft-prometheus\n:9090"]
        GRAF["aircraft-grafana\n:3000"]
        NODE["aircraft-node-exporter\n:9100"]
        REDEX["aircraft-redis-exporter\n:9121"]
    end

    PROD --> SOL --> KC --> KF --> TM
    JM --- TM
    TM --> RD
    RD --> API --> FE
    API & NODE & REDEX --> PROM --> GRAF

    style SOL fill:#4a1d96,color:#fff
    style KF fill:#f59e0b,color:#000
    style TM fill:#0369a1,color:#fff
    style RD fill:#991b1b,color:#fff
    style API fill:#15803d,color:#fff
    style GRAF fill:#ea580c,color:#fff
```

![Grafana Production Observability](../assets/grafana.png)

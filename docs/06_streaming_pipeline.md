# Streaming Pipeline Specification

## Distributed Telemetry Ingestion, Apache Flink 2.0 & RocksDB State Management

The streaming architecture ingests high-frequency turbofan sensor telemetry across a simulated 100-engine fleet, bridges events across an enterprise message fabric, and executes stateful sliding window aggregations with exactly-once fault tolerance.

---

## 🏗️ End-to-End Streaming Topology

```mermaid
flowchart TD
    PROD["Telemetry Producer\n100 Engines · Risk-Distributed\nMonotonic Cycle Tracking"]

    subgraph Ingestion["Ingestion Fabric"]
        SOL["Solace PubSub+\nSMF Binary Transport :55555\nTopic: aircraft/engine/+/telemetry/cycle"]
        CONN["Solace Kafka Connector\nManaged Source Connector :8083\nAutomated Queue Bridge"]
    end

    subgraph Buffer["Durable Event Log"]
        KF["Apache Kafka (KRaft Mode)\nTopic: telemetry.raw · 3 Partitions\nRetention: 24h · Key: engine_id"]
    end

    subgraph Processing["Stateful Processing Engine"]
        FLINK["Apache Flink 2.0 (PyFlink Cluster)\nKafkaSource · Exactly-Once Checkpoints (60s)\nEmbeddedRocksDBStateBackend"]
        NORM["Stateless NormalizeMap\nMinMax Global Scaler Transformation"]
        KEY["keyBy(engine_id)\nPartitioned by Engine ID"]
        WINDOW["RollingWindowProcess\nKeyed ListState Buffer (30 Cycles)\nSliding Eviction on Full Window"]
    end

    subgraph Sinks["Dual Storage Sinks"]
        REDIS["Redis Feature Store\nKey: engine:{id}:features\n330 float32 bytes · 1h TTL"]
        S3["S3 Parquet Sink\nHive-Partitioned (date/hour)\nCheckpoint-Aligned Flush"]
    end

    PROD -->|SMF Publish| SOL
    SOL -->|Managed Queue| CONN
    CONN -->|Partitioned Records| KF
    KF -->|Stream Ingest| FLINK
    FLINK --> NORM --> KEY --> WINDOW
    WINDOW -->|Online Inference Features| REDIS
    WINDOW -->|Historical Partitioned Storage| S3

    style SOL fill:#4a1d96,color:#fff
    style CONN fill:#7c3aed,color:#fff
    style KF fill:#f59e0b,color:#000
    style FLINK fill:#0369a1,color:#fff
    style REDIS fill:#991b1b,color:#fff
    style S3 fill:#15803d,color:#fff
```

![Streaming Pipeline Topology](../assets/pipelinepage.png)

---

## 1. Multi-Tier Ingestion Rationale

Early architectural designs routed telemetry directly into consumer processes or volatile broker queues. This created compounding operational failures under high load:

* **Durability vs. Telemetry Mismatch**: In-memory message queues lack configurable retention and replay semantics on restart, causing unrecoverable data loss during pipeline maintenance.
* **Decoupled JVM Bridging**: The Solace-to-Kafka connector runs as a managed source connector within Kafka Connect, completely isolating message broker protocols from the application runtime.
* **Pure Python Stream Reliability**: Flink consumes from Kafka via `KafkaSource` using standard Kafka consumer protocols, eliminating unstable JNI/JNDI bridges inside Python worker containers.

---

## 2. Component Specifications

### A. Telemetry Fleet Producer
* **Simulated Fleet Scale**: 100 concurrent turbofan engines.
* **Risk-Distributed State**: Offsets ensure a continuous, realistic fleet distribution:
  * **70% LOW Risk** (Nominal degradation, early lifecycle)
  * **10% MEDIUM Risk** (Noticeable wear trends)
  * **10% HIGH Risk** (Accelerated degradation curve)
  * **10% CRITICAL Risk** (Terminal failure horizon, $RUL < 30$)
* **Batch Throttle**: Inter-message delay is evaluated **once per round of 100 engines**, ensuring all engines populate their 30-cycle rolling windows synchronously on cold start.

### B. Solace PubSub+ Message Broker
* **Wire Protocol**: Solace Message Format (SMF) over binary port `:55555`.
* **Topic Taxonomy**: `aircraft/engine/{engine_id}/telemetry/cycle`
* **Wildcard Subscription**: `aircraft/engine/+/telemetry/cycle` routes all 100 aircraft streams into the durable spool `flink.feature.processor`.

### C. Apache Kafka Event Log
* **Cluster Architecture**: KRaft mode (Zero ZooKeeper dependencies).
* **Topic Configuration**: `telemetry.raw` partitioned into 3 independent shards matching 3 Flink task slots.
* **Partition Affinity**: `engine_id` is assigned as the Kafka message key, guaranteeing strict in-order delivery per engine across network rebalances.

### D. PyFlink 2.0 Streaming Engine
* **Execution Environment**: Distributed cluster (JobManager `:8082`, TaskManager with 3 task slots).
* **State Management**: `EmbeddedRocksDBStateBackend` storing keyed `ListState` out-of-core. This enables fast incremental checkpointing without garbage-collection pauses.
* **Window Mechanics**: Keyed process function maintains an ordered sliding buffer of length $T = 30$. When cycle count reaches 30, the operator emits a `FeatureVector` and slides forward by dropping the oldest cycle index.

---

## 3. Dual Sink Contracts

```mermaid
flowchart LR
    WIN[RollingWindowProcess] --> RD[Redis Feature Store]
    WIN --> S3[Amazon S3 Parquet]

    RD -->|sub-millisecond reads| API[FastAPI Inference Engine]
    S3 -->|columnar historical scans| RETRAIN[Batch Pipeline Retraining]

    style RD fill:#991b1b,color:#fff
    style S3 fill:#15803d,color:#fff
```

| Dimension | Online Store (Redis) | Offline Store (Amazon S3) |
| :--- | :--- | :--- |
| **Target Audience** | Low-latency inference serving | Offline retraining & drift baseline |
| **Record Format** | Binary IEEE-754 `float32` (1320 bytes) | Snappy-compressed Apache Parquet |
| **Key / Path Schema** | `engine:{id}:features` | `features/date=YYYY-MM-DD/hour=HH/` |
| **Write Guarantee** | Atomic pipeline `MSET` (idempotent) | Checkpoint-aligned file flush |
| **Eviction Policy** | **3600s TTL** (automatic stale engine pruning) | Immutable long-term retention |

---

## 4. End-to-End Exactly-Once Guarantees

```mermaid
flowchart LR
    P[Producer → Solace] -->|At-Least-Once| B[Solace → Kafka]
    B -->|At-Least-Once| K[Kafka Log]
    K -->|Exactly-Once Offsets| F[PyFlink Checkpoint]
    F -->|Idempotent Overwrite| R[Redis Feature Store]
    F -->|Checkpoint Commit| S[S3 Parquet]

    style F fill:#0369a1,color:#fff
    style R fill:#991b1b,color:#fff
    style S fill:#15803d,color:#fff
```

| Boundary Segment | Technical Mechanism | Guarantee Level |
| :--- | :--- | :--- |
| **Producer $\rightarrow$ Solace** | SMF broker acknowledgment | **At-Least-Once** |
| **Solace $\rightarrow$ Kafka** | Kafka Connect record offset commit | **At-Least-Once** |
| **Kafka $\rightarrow$ PyFlink** | Checkpoint-committed Kafka consumer offsets | **Exactly-Once** |
| **PyFlink $\rightarrow$ Redis** | Key-deterministic idempotent `MSET` overwrite | **Effectively Exactly-Once** |
| **PyFlink $\rightarrow$ S3** | `notifyCheckpointComplete` transactional flush | **Exactly-Once** |

> **Convergence Property**: Because Redis keys are keyed deterministically by engine ID (`engine:ENG-042:features`), upstream network retries safely overwrite identical tensor bytes, guaranteeing consistent inference features at all times.

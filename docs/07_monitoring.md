# Monitoring & Observability Specification

## Distributed System Telemetry, Grafana Dashboards & Sensor Drift Analysis

The platform implements a multi-tier observability architecture spanning infrastructure performance, microservice health, and statistical machine learning drift.

---

## 🔭 Multi-Tier Observability Architecture

```mermaid
flowchart TD
    subgraph Instrumentation["Telemetry Sources"]
        API["FastAPI :8000\n9 Custom Prometheus Gauges & Counters"]
        NODE["Node Exporter :9100\nCPU · Memory · Disk IO"]
        REDIS["Redis Exporter :9121\nKey Cardinality · Evictions · Memory"]
    end

    subgraph MetricsCollector["Time Series Storage"]
        PROM["Prometheus Engine :9090\n15-Second Automated Scrape Loop"]
    end

    subgraph VisualObservability["Visualization"]
        GRAF["Grafana Enterprise :3000\n15+ Pre-Provisioned Production Panels"]
    end

    subgraph MLDrift["ML Observability"]
        EVID["Evidently AI 0.7 Engine\nKolmogorov-Smirnov (KS) Two-Sample Drift Test"]
        REPORTS["HTML Visual Reports\nMounted into Container at /drift/reports/"]
        MODAL["Operations UI\nIn-Dashboard Fullscreen Modal"]
    end

    API & NODE & REDIS --> PROM
    PROM --> GRAF
    EVID --> REPORTS --> MODAL

    style PROM fill:#c2410c,color:#fff
    style GRAF fill:#ea580c,color:#fff
    style EVID fill:#4f46e5,color:#fff
```

![Grafana Dashboard Overview](../assets/grafana.png)

---

## 1. Prometheus Telemetry Matrix

Custom metrics instrumented within `src/inference/metrics.py` and exposed at `GET /metrics`:

| Metric Identifier | Class | Labels | Operational Semantics |
| :--- | :--- | :--- | :--- |
| `active_engines_total` | **Gauge** | None | Real-time count of engines maintaining active feature vectors in Redis |
| `critical_engines_total` | **Gauge** | None | Instantaneous count of engines operating within the critical failure envelope |
| `prediction_requests_total` | **Counter** | `engine_id`, `risk_level` | Monotonic count of total inferences scored across all pathways |
| `prediction_latency_seconds` | **Histogram** | None | Execution duration of the batched GRU model inference pass |
| `predicted_rul_cycles` | **Histogram** | None | Empirical distribution of denormalized RUL predictions across the fleet |
| `failure_risk_score` | **Histogram** | None | Distribution of fleet failure risk scores $[0.0, 1.0]$ |
| `prediction_confidence` | **Histogram** | None | Epistemic confidence scores derived from Monte Carlo Dropout variance |
| `prediction_errors_total` | **Counter** | `error_type` | Diagnostic counter capturing tensor shape mismatches or Redis timeouts |
| `model_load_time_seconds` | **Gauge** | None | Cold-start model deserialization and warm-up latency |

---

## 2. Automated Alerting Matrix

Rules defined within `monitoring/prometheus/alerting_rules.yml`:

| Alert Identifier | Threshold Expression | Evaluation Duration | Severity Level |
| :--- | :--- | :--- | :--- |
| `CriticalEngineDetected` | `rate(critical_engines_total[5m]) > 0` | 1 minute | **CRITICAL** |
| `HighPredictionLatency` | `p95(prediction_latency) > 100ms` | 5 minutes | **WARNING** |
| `HighPredictionErrorRate` | `rate(prediction_errors_total[5m]) > 0.01/s` | 5 minutes | **WARNING** |
| `RedisHighMemoryUsage` | `redis_memory_used / redis_memory_max > 80%` | 5 minutes | **WARNING** |
| `InferenceEngineDown` | `up{job="inference-api"} == 0` | 1 minute | **CRITICAL** |

---

## 3. Evidently AI 0.7 Sensor Drift Engine

Sensor drift indicates physical turbofan wear, calibration degradation, or operating condition shifts:

```mermaid
flowchart LR
    REF["Baseline Gold Distribution\nTraining Sensor Feature Matrices"] --> DRIFT["Evidently 0.7 Engine\nsrc/monitoring/drift_detector.py"]
    CUR["Live Streaming Telemetry\nRecent Windowed Engine Records"] --> DRIFT

    DRIFT --> KS["Kolmogorov-Smirnov Test\np-value < 0.05 Threshold per Sensor"]
    DRIFT --> PRESET["DataDriftPreset Snapshot\nsnapshot.save_html()"]
    PRESET --> HTML["Interactive HTML Report\nServed at GET /drift/reports"]
    HTML --> UI["Operations UI\nMLOps Drift Inspection Tab"]

    style DRIFT fill:#4338ca,color:#fff
    style UI fill:#15803d,color:#fff
```

### Statistical Verification
* **Algorithm**: Non-parametric two-sample Kolmogorov-Smirnov (KS) test per sensor channel.
* **Null Hypothesis ($H_0$)**: Incoming telemetry samples originate from the baseline training distribution.
* **Drift Threshold**: Feature flagged as drifted if KS test $p$-value $< 0.05$.
* **Evidently 0.7 API Contract**: `report.run()` returns a `Snapshot` object, calling `snapshot.save_html()` on the snapshot directly.

---

## 4. Structured Operational Logging

The inference engine emits JSON structured log events with consistent audit fields:

| Field | Type | Description |
| :--- | :--- | :--- |
| `timestamp` | ISO-8601 | UTC microsecond timestamp |
| `level` | String | Log severity (`INFO`, `WARNING`, `ERROR`) |
| `engine_id` | String | Unique aircraft engine identifier |
| `rul` | Float | Predicted remaining flight cycles |
| `failure_risk` | Float | Normalized risk score $[0.0, 1.0]$ |
| `risk_level` | String | Categorical status (`LOW`, `MED`, `HIGH`, `CRITICAL`) |
| `confidence` | Float | Bayesian confidence index $[0.0, 1.0]$ |
| `latency_ms` | Float | End-to-end forward pass execution time |

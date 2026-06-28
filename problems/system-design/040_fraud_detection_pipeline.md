# Problem 040: Design a Real-Time Fraud Detection Pipeline

**Difficulty:** Hard  
**Topics:** System Design, Streaming, ML, Real-time Processing  
**Company Tags:** Stripe, Square, PayPal, Revolut, Any Fintech

## Problem Statement

Design a real-time fraud detection system for a payment processing company that handles 10,000 transactions per second. The system must:

1. Detect fraudulent transactions in under 100ms
2. Handle both rule-based and ML-based detection
3. Support real-time alerting
4. Store data for historical analysis and model training
5. Handle 99.99% uptime requirement

Walk through: **Ingestion → Processing → Detection → Alerting → Storage**

## Requirements

**Functional:**
- Classify transactions as fraud/not-fraud in real-time
- Support rule-based detection (velocity checks, geo-anomalies)
- Support ML model inference
- Alert fraud analysts for manual review
- Allow rules/models to be updated without downtime

**Non-functional:**
- Latency: < 100ms end-to-end
- Throughput: 10K TPS, bursts to 50K TPS
- Availability: 99.99%
- Data retention: 7 years for compliance

## Hints

<details>
<summary>Hint 1: Ingestion Layer</summary>
Consider Kafka for high-throughput, durable ingestion. Partition by user_id for ordering guarantees.
</details>

<details>
<summary>Hint 2: Processing Strategy</summary>
Use a tiered approach: fast rules first (< 10ms), then ML model (< 50ms). Fail open vs fail closed?
</details>

<details>
<summary>Hint 3: Feature Store</summary>
Pre-compute aggregates (transactions in last hour, avg transaction amount) in a low-latency store like Redis.
</details>

## Solution

<details>
<summary>Click to reveal solution</summary>

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           FRAUD DETECTION PIPELINE                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────┐    ┌──────────┐    ┌──────────────────────────────────────┐   │
│  │ Payment  │───▶│  Kafka   │───▶│         Stream Processor             │   │
│  │   API    │    │ (ingest) │    │        (Flink / Spark)               │   │
│  └──────────┘    └──────────┘    │                                      │   │
│                        │         │  ┌────────────┐  ┌────────────────┐  │   │
│                        │         │  │ Rule Engine│  │  ML Inference  │  │   │
│                        │         │  │  (< 10ms)  │  │   (< 50ms)     │  │   │
│                        ▼         │  └──────┬─────┘  └───────┬────────┘  │   │
│               ┌──────────────┐   │         │                │           │   │
│               │ Feature Store│◀──┼─────────┴────────────────┘           │   │
│               │   (Redis)    │   │                                      │   │
│               └──────────────┘   └───────────────┬──────────────────────┘   │
│                                                  │                          │
│                                    ┌─────────────┼─────────────┐            │
│                                    ▼             ▼             ▼            │
│                             ┌──────────┐  ┌──────────┐  ┌──────────┐        │
│                             │  Kafka   │  │  Alerts  │  │  Delta   │        │
│                             │ (scored) │  │ (PagerD) │  │  Lake    │        │
│                             └────┬─────┘  └──────────┘  └──────────┘        │
│                                  │                                          │
│                                  ▼                                          │
│                          ┌──────────────┐                                   │
│                          │  Dashboard   │                                   │
│                          │  (Grafana)   │                                   │
│                          └──────────────┘                                   │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Component Deep Dive

**1. Ingestion Layer (Kafka)**

```
Topic: transactions
Partitions: 100 (partition by user_id for ordering)
Replication: 3
Retention: 7 days (before archival)

Message Schema:
{
  "transaction_id": "txn_abc123",
  "user_id": "user_456",
  "merchant_id": "merch_789",
  "amount": 150.00,
  "currency": "USD",
  "timestamp": "2024-01-15T10:30:00Z",
  "device_fingerprint": "fp_xyz",
  "ip_address": "192.168.1.1",
  "geo_location": {"lat": 37.7749, "lon": -122.4194}
}
```

**2. Feature Store (Redis)**

Pre-computed aggregates for low-latency lookups:

```python
# Features computed in real-time (sliding windows)
features = {
    "user:{user_id}:txn_count_1h": 5,
    "user:{user_id}:txn_count_24h": 23,
    "user:{user_id}:avg_amount_7d": 85.50,
    "user:{user_id}:unique_merchants_24h": 3,
    "user:{user_id}:last_location": "37.7749,-122.4194",
    "device:{fingerprint}:user_count": 1,
    "merchant:{id}:fraud_rate_30d": 0.02
}
```

**3. Rule Engine (< 10ms)**

Fast rules that don't need ML:

```python
RULES = [
    # Velocity checks
    Rule("high_velocity", 
         condition=lambda f: f["txn_count_1h"] > 10,
         action="flag", score=0.3),
    
    # Amount anomaly
    Rule("amount_spike",
         condition=lambda f, txn: txn["amount"] > f["avg_amount_7d"] * 5,
         action="flag", score=0.4),
    
    # Geo-anomaly (impossible travel)
    Rule("impossible_travel",
         condition=lambda f, txn: distance(f["last_location"], txn["geo"]) > 500 
                                  and time_diff < 1_hour,
         action="block", score=0.9),
    
    # Known bad device
    Rule("device_shared",
         condition=lambda f: f["device_user_count"] > 3,
         action="flag", score=0.5),
]
```

**4. ML Inference (< 50ms)**

```python
# Model: XGBoost or LightGBM for low latency
# Features: ~50 features from feature store + transaction
# Deployed via: TensorFlow Serving / Triton / SageMaker

class FraudModel:
    def predict(self, features: dict) -> float:
        # Returns probability [0, 1]
        return self.model.predict_proba(features)[0][1]

# Decision thresholds
THRESHOLDS = {
    "auto_block": 0.95,    # Block immediately
    "manual_review": 0.70,  # Send to analyst
    "monitor": 0.30,        # Log but allow
    "allow": 0.0            # No action
}
```

**5. Stream Processing (Flink/Spark)**

```python
# Flink pseudocode
transactions
    .keyBy("user_id")
    .process(EnrichWithFeatures())      # Fetch from Redis
    .process(ApplyRules())               # Rule engine
    .filter(lambda t: t.rule_score < 0.9)  # Skip if already blocked
    .process(MLInference())              # ML scoring
    .process(MakeDecision())             # Combine scores
    .addSink(KafkaSink("scored_transactions"))
    .addSink(AlertSink())                # PagerDuty/Slack
    .addSink(DeltaLakeSink())            # Historical storage
```

**6. Decision & Alerting**

```python
def make_decision(transaction, rule_score, ml_score):
    # Combine scores (weighted average or max)
    final_score = max(rule_score, ml_score * 0.8)
    
    if final_score >= 0.95:
        return Decision.BLOCK, alert_level="critical"
    elif final_score >= 0.70:
        return Decision.REVIEW, alert_level="high"
    elif final_score >= 0.30:
        return Decision.ALLOW_MONITOR, alert_level=None
    else:
        return Decision.ALLOW, alert_level=None
```

**7. Storage Layer**

```
Hot Storage (< 30 days):
  - Delta Lake on S3/ADLS
  - Partitioned by date, hour
  - Used for dashboards, recent analysis

Cold Storage (30 days - 7 years):
  - Parquet on S3 Glacier
  - Compressed, rarely accessed
  - Compliance retention
```

### Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Fail open vs closed | Fail open (allow) | Business priority: don't block good transactions |
| Sync vs async ML | Sync (in-path) | Latency requirement allows it |
| Feature freshness | Real-time + batch | Velocity = real-time, historical = batch |
| Model update | Shadow mode first | Deploy new model in parallel, compare before switching |

### Handling Edge Cases

1. **Model unavailable:** Fall back to rules only
2. **Feature store unavailable:** Use cached/default features
3. **Kafka lag:** Auto-scale consumers, alert if > 1 min
4. **False positives:** Human review loop, feedback to model

</details>

## Follow-up Questions

1. **How do you handle model drift?** Monitor feature distributions and model performance
2. **How do you A/B test fraud models?** Shadow mode, then gradual rollout
3. **What about privacy (GDPR)?** Anonymize features, retention policies
4. **How do you prevent adversarial attacks?** Feature obfuscation, model ensembles

## What Interviewers Look For

1. **End-to-end thinking:** Covers all components, not just happy path
2. **Latency awareness:** Knows where milliseconds matter
3. **Trade-off analysis:** Fail open vs closed, sync vs async
4. **Operational concerns:** Monitoring, alerting, model updates

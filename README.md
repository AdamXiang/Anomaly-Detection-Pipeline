# IoT Realtime Pipeline — Anomaly Detection

![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python)
![Kafka](https://img.shields.io/badge/Kafka-3.6_KRaft-231F20?logo=apachekafka)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?logo=postgresql)
![Grafana](https://img.shields.io/badge/Grafana-10.2-F46800?logo=grafana)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)
![License](https://img.shields.io/badge/License-MIT-green)

## ⚡ One-liner

A production-grade IoT streaming pipeline that simulates 50 smartwatch devices at up to 10,000 msg/sec, backed by a 3-broker Kafka KRaft cluster, a fault-tolerant Python consumer, and a full observability stack — all reproducible with a single `docker compose up`.

---

## 🏗️ System Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        IoT Data Generator                           │
│          50 virtual smartwatches × 100–10,000 msg/sec               │
└───────────────────────────┬─────────────────────────────────────────┘
                            │ JSON over Kafka Protocol
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Kafka Cluster (KRaft Mode)                       │
│      kafka-1 :9092   kafka-2 :9093   kafka-3 :9094                  │
│      replication.factor=3  |  min.insync.replicas=2                 │
└───────────────────────────┬─────────────────────────────────────────┘
                            │ Micro-batch (500 msgs or 5s)
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│              Python Consumer (At-Least-Once + Idempotent)           │
│         Bulk INSERT via psycopg2 · ON CONFLICT DO NOTHING           │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                PostgreSQL 15 (Partitioned by Day)                   │
│          iot_data.athlete_telemetry  ·  vw_ingestion_lag            │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
          ┌─────────────────┼──────────────────┐
          ▼                 ▼                  ▼
   kafka-exporter    postgres-exporter   node-exporter
          └─────────────────┬──────────────────┘
                            ▼
                       Prometheus
                            │
                            ▼
                   Grafana Dashboards
```

**Data Flow (5 stages):**

1. **Generator** — Simulates 50 devices in two modes: `normal` (~100 msg/sec, resting heart rate) and `burst` (~10,000 msg/sec, marathon simulation). Each message is keyed by `device_id` to guarantee per-device ordering across Kafka partitions.
2. **Kafka Cluster** — A 3-broker KRaft cluster (no ZooKeeper) with topic replication across all brokers. Tolerates 1 broker failure without data loss.
3. **Consumer** — Dual-trigger micro-batch strategy: flush when buffer hits 500 records **or** 5 seconds elapse. Commits Kafka offset only **after** a successful PostgreSQL write.
4. **PostgreSQL** — Daily range partitions on `event_time`. Records both `event_time` (device clock) and `ingestion_time` (server clock) to compute end-to-end pipeline lag.
5. **Observability Stack** — Prometheus scrapes three exporters every 15 seconds. Grafana auto-provisions two dashboards on startup via the provisioning directory.

---

## 🎯 Anomaly Simulation Results

Three injectable failure scenarios, each with a dedicated script under `python/anomaly/`.

| # | Scenario | Trigger | Peak Metric Observed | Recovery Mechanism |
|---|---|---|---|---|
| 1 | **Broker Failure** | `docker compose stop kafka-3` | Active brokers: 3 → 2 · Under-Replicated Partitions spike | KRaft leader re-election · `docker compose start kafka-3` |
| 2 | **Consumer Crash** | Kill consumer process | Consumer Lag: **1,250,000 msgs** | Restart consumer → cliff-drop recovery via high-throughput batch catch-up |
| 3 | **DB Pressure** | 80 concurrent connections | PG Active Connections: 0 → 80 | Graceful drain to 0 after script completion |

**Anomaly 1 — Broker Failure (Under-Replicated Partitions)**
<img width="1440" height="857" alt="截圖 2026-03-10 下午2 17 05" src="https://github.com/user-attachments/assets/5f18e28f-ae6f-491f-862e-5bdaf99c2ee2" />

**Anomaly 2 — Consumer Lag Spike & Recovery (The Main Event)**
<img width="1440" height="462" alt="截圖 2026-03-10 下午2 18 44" src="https://github.com/user-attachments/assets/888d310c-5036-4eca-946e-3e992ad438b7" />


**Anomaly 3 — PostgreSQL Connection Pressure**
<img width="973" height="267" alt="截圖 2026-03-10 下午2 19 40" src="https://github.com/user-attachments/assets/b7293e20-78b3-4567-827d-fd4a44625272" />
<img width="427" height="267" alt="截圖 2026-03-10 下午2 20 02" src="https://github.com/user-attachments/assets/7774037e-94e4-42bd-8e7c-f57fbdbcdbc2" />

---

## 🔑 Key Design Decisions

**1. KRaft over ZooKeeper**
KRaft (Kafka Raft Metadata) eliminates ZooKeeper as a separate dependency, removing a classic single point of failure and saving ~300MB of RAM per deployment. All three brokers act as both broker and controller, forming a Raft quorum for leader election.

**2. At-Least-Once delivery + Idempotent writes**
The consumer commits Kafka offsets only **after** a successful PostgreSQL write — never before. This guarantees zero data loss on consumer restart at the cost of potential redelivery. `ON CONFLICT DO NOTHING` on the insert makes redelivery harmless, achieving effective exactly-once semantics at the storage layer without distributed transactions.

**3. Partition key = `device_id`**
Routing all messages from the same device to the same partition ensures strict time-ordering per device. A consumer reading partition 0 will always see watch-A's telemetry in the correct sequence — critical for accurate lag and heart rate trend calculations.

**4. Dual-trigger micro-batch (size=500, timeout=5s)**
A size-only trigger causes severe data latency during off-peak hours (a single batch might take minutes to fill). A timeout-only trigger is inefficient under high load. The dual condition handles both extremes: high throughput gets the bulk-insert efficiency of 500-row batches; low throughput gets a guaranteed 5-second maximum buffer age.

---

## 📋 Prerequisites

| Requirement | Version |
|---|---|
| Docker Desktop | ≥ 4.x, **minimum 6GB RAM allocated** |
| Python | 3.12+ |
| pip | Latest |

> [!WARNING]
> Before starting, ensure Docker Desktop → Settings → Resources → Memory is set to at least **6GB**. The 3-broker Kafka cluster alone reserves ~1.5GB.

---

## ⚙️ Quick Start

**Step 1 — Initialize the environment**

```bash
chmod +x setup.sh && ./setup.sh
```

This script generates a valid Kafka KRaft Cluster ID, writes `.env`, and creates all required config directories. Run it once before the first `docker compose up`.

**Step 2 — Start the infrastructure (staged startup recommended)**

```bash
# Stage 1: Database
docker compose up -d postgres

# Stage 2: Kafka cluster (wait ~90s for KRaft leader election)
docker compose up -d kafka-1 kafka-2 kafka-3

# Stage 3: Exporters
docker compose up -d kafka-exporter node-exporter postgres-exporter

# Stage 4: Observability stack
docker compose up -d prometheus
docker compose up -d grafana
```

> Staged startup prevents exporters from failing their health checks before Kafka brokers are ready.

**Step 3 — Start the data pipeline**

```bash
cd python
pip install -r requirements.txt

# Terminal 1: Start the producer (normal mode, 100 msg/sec)
python pipeline/producer.py

# Terminal 2: Start the consumer
python pipeline/consumer.py
```

**Step 4 — Open Grafana**

Navigate to [http://localhost:3000](http://localhost:3000) with credentials `admin / admin123`.

Two dashboards are auto-provisioned:
- **IoT Overview** — Heart rate, SpO₂, E2E lag, throughput, battery levels
- **Infrastructure Monitoring** — Kafka brokers, consumer lag, PostgreSQL connections, CPU/memory

**Step 5 — Inject an anomaly**

```bash
# Anomaly 1: Kill a Kafka broker
bash python/anomaly/anomaly_1_broker_failure.sh

# Anomaly 2: Simulate consumer crash (run, then Ctrl+C the consumer in Terminal 2)
bash python/anomaly/anomaly_2_consumer_lag.sh

# Anomaly 3: DB connection pressure
bash python/anomaly/run_anomaly_3.sh
```

---

## 📁 Folder Structure

```
.
├── docker-compose.yml          # Full infrastructure definition (Kafka + PG + Observability)
├── setup.sh                    # One-time environment initializer
├── .env.template               # Environment variable reference
│
├── grafana/
│   ├── dashboards/             # Auto-provisioned dashboard JSON files
│   └── provisioning/           # Grafana datasource & dashboard loader config
│
├── postgres/
│   └── init.sql                # Schema, partitions, indexes, views, roles
│
├── prometheus/
│   └── prometheus.yml          # Scrape targets (Kafka, PG, Node, Grafana)
│
└── python/
    ├── config.py               # Kafka & PostgreSQL connection config
    ├── requirements.txt
    ├── anomaly/                # Three injectable failure scenario scripts
    ├── generator/
    │   └── data_generator.py   # IoT device simulator (normal + burst modes)
    ├── pipeline/
    │   ├── producer.py         # Confluent Kafka producer with delivery tracking
    │   └── consumer.py         # Micro-batch consumer with idempotent PG writer
    └── utils/
        └── logger.py           # Loguru logger setup
```

---

## 🔭 Production Roadmap

This project is a validated proof-of-concept. The following are the architectural upgrades required before a production deployment:

- [ ] **PgBouncer** — Connection pooling layer in front of PostgreSQL to absorb connection spikes (as observed in Anomaly 3) and prevent `FATAL: sorry, too many clients` under sustained load
- [ ] **TimescaleDB** — Drop-in replacement for PostgreSQL (zero consumer code changes required) to unlock `Hypertable` auto-partitioning and `time_bucket()` for high-performance OLAP queries on time-series data
- [ ] **Kubernetes + HPA** — Containerize the consumer as a Deployment; use Horizontal Pod Autoscaler triggered by `kafka_consumergroup_lag` to dynamically scale consumer replicas during burst traffic
- [ ] **Schema Registry** — Enforce Avro/Protobuf schema evolution on the Kafka topic to prevent malformed producer payloads from breaking the consumer
- [ ] **Dead Letter Queue (DLQ)** — Route unparseable messages to a separate topic instead of silently discarding them

---

## 📄 License

MIT

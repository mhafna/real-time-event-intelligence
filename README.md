# Real-Time AI Event Intelligence Platform

A proof-of-concept streaming intelligence platform for processing smart-meter events in near real time.

The system replays historical smart-meter events as a live stream, processes them through Apache Kafka and Apache Flink, applies pluggable operational and machine-learning use cases, persists results in ClickHouse, and visualizes analytics in Grafana.

## Architecture

Historical Smart-Meter Events
→ Python Replay Producer
→ Apache Kafka
→ Apache Flink
→ Feature Engineering and Enrichment
→ Plugin-Based Use Cases
→ Kafka Result Topics
→ ClickHouse
→ Grafana

## Technology Stack

- Python
- Java
- Apache Kafka
- Apache Flink
- ClickHouse
- Grafana
- Docker / Docker Compose
- scikit-learn

## Implemented Use Cases

### Outage Intelligence

Stateful streaming logic detects power-failure occurrences, restorations, and unresolved outages.

The use case maintains meter-level outage state and supports supply-reliability analytics.

### Topology Inference

A lightweight topology-inference use case ranks likely feeder relationships for meters with incomplete topology information.

Candidate ranking combines recent event co-occurrence and geographic proximity.

The topology score is a heuristic ranking score and is not interpreted as a probability.

Detailed notes:

`docs/topology_status_2026-08-17.md`

### Anomaly Detection

An Isolation Forest model identifies unusual combinations of 15-minute smart-meter features.

Active model features:

- `event_count_15m`
- `avg_voltage_15m`
- `voltage_range_15m`

The model is trained offline in Python, exported to a portable JSON representation, and scored inside the Java/Flink runtime.

Validated runtime result:

- 100 replayed source events
- 58 feature windows
- 6 anomalous windows
- 52 normal windows
- Python-to-Java prediction parity: 58 / 58
- Maximum Python-to-Java anomaly-score difference: 0.0

Detailed notes:

`docs/anomaly_detection_status_2026-08-19.md`

## Plugin Architecture

Use cases are configured through:

`flink-java/src/main/resources/usecases.json`

Each use case implements the common plugin interface and produces its own result stream.

The architecture allows a new use case to be introduced through a plugin implementation and configuration entry without modifying the core event-processing pipeline.

## Shared 15-Minute Features

The streaming feature pipeline generates:

- `event_count_15m`
- `avg_voltage_15m`
- `voltage_range_15m`
- `power_failure_count_15m`

Processing uses event-time timestamps and 15-minute tumbling windows.

## Main Kafka Topics

- `smart-meter-events`
- `processed-smart-meter-events`
- `meter-features-15m`
- `outage-incidents`
- `topology-inference-results`
- `anomaly-detection-results`

Historical result topics use Kafka append-time handling where required so historical event timestamps do not interfere with topic retention.

## ClickHouse

Result streams use the persistence pattern:

Kafka Engine table
→ Materialized View
→ MergeTree permanent table

Persisted datasets include:

- processed events
- 15-minute meter features
- outage incidents
- topology inference results
- anomaly detection results

## Grafana

The dashboard provides operational and analytical views for:

- streaming event processing
- outage and supply-reliability analytics
- topology inference
- detected anomalies
- anomalous meter windows
- anomaly prediction breakdown
- anomaly-score ranking

## Running the Platform

Start the infrastructure from the project root with:

`docker compose up -d`

Verify containers with:

`docker ps`

Local interfaces:

- Flink: `http://localhost:8081`
- Grafana: `http://localhost:3000`
- ClickHouse HTTP: `http://localhost:8123`
- Kafka host listener: `localhost:9092`

## Build the Flink Runtime

The Java runtime is built from `flink-java/` using Maven in Docker.

The production shaded JAR is:

`flink-java/target/event-intelligence-flink-1.0-SNAPSHOT.jar`

The validated final anomaly-enabled runtime is also backed up under:

`flink-java/backups/2026-08-19-final-anomaly-runtime/`

## Replay Historical Events

Historical event data is replayed using:

`producer/replay_producer.py`

The producer supports accelerated replay while preserving original event timestamps for Flink event-time processing.

## Safe Shutdown

Cancel the active Flink job before shutting down the environment.

Then use:

`docker compose stop`

Avoid `docker compose down -v` unless persistent local data is intentionally being deleted.

## Documentation

Additional implementation notes are available under `docs/`:

- `day2_checkpoint.md`
- `topology_status_2026-08-17.md`
- `anomaly_detection_status_2026-08-19.md`

## Current Status

The core streaming platform and three representative use cases have been implemented and validated.

The current build demonstrates historical-to-real-time replay, Kafka streaming, Flink event-time processing, reusable feature engineering, stateful processing, plugin-based extensibility, topology inference, Java-integrated machine learning, ClickHouse persistence, and Grafana analytics.
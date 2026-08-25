-- Real-Time AI Event Intelligence Platform
-- Active ClickHouse schema
-- Validated runtime snapshot: 2026-08-19

CREATE DATABASE IF NOT EXISTS event_intelligence;

-- ------------------------------------------------------------
-- processed_events
-- ------------------------------------------------------------
CREATE TABLE event_intelligence.processed_events (`msn` Nullable(String), `msn_id` Nullable(Int64), `evnt_id` Nullable(Int32), `event_ts` Nullable(String), `log_seq_no` Nullable(Int64), `v_r` Nullable(Float64), `v_y` Nullable(Float64), `v_b` Nullable(Float64), `validation_status` String, `validation_reason` Nullable(String), `processed_at` DateTime DEFAULT now(), `event_catalogue_match` Nullable(Bool), `event_tblrefid` Nullable(Int32), `event_name` Nullable(String), `event_state` Nullable(String), `isrestoration` Nullable(Bool), `priorityname` Nullable(String), `eventclassification_name` Nullable(String), `hierarchy_match` Nullable(Bool), `msn_normalized` Nullable(String), `dtr_name` Nullable(String), `dtr_network_code` Nullable(String), `feeder_name` Nullable(String), `substation_name` Nullable(String)) ENGINE = MergeTree ORDER BY processed_at SETTINGS index_granularity = 8192
;

-- ------------------------------------------------------------
-- meter_features_15m
-- ------------------------------------------------------------
CREATE TABLE event_intelligence.meter_features_15m (`msn` String, `window_start` DateTime, `window_end` DateTime, `event_count_15m` UInt64, `avg_voltage_15m` Nullable(Float64), `voltage_range_15m` Nullable(Float64), `power_failure_count_15m` UInt64, `ingested_at` DateTime DEFAULT now()) ENGINE = MergeTree ORDER BY (msn, window_start) SETTINGS index_granularity = 8192
;

-- ------------------------------------------------------------
-- outage_incident_events
-- ------------------------------------------------------------
CREATE TABLE event_intelligence.outage_incident_events (`incident_id` String, `msn` String, `event_name` String, `event_tblrefid` Int32, `incident_status` String, `start_ts` Nullable(String), `end_ts` Nullable(String), `duration_minutes` Nullable(Float64), `dtr_name` Nullable(String), `dtr_network_code` Nullable(String), `feeder_name` Nullable(String), `substation_name` Nullable(String), `received_at` DateTime DEFAULT now()) ENGINE = MergeTree ORDER BY (incident_id, received_at) SETTINGS index_granularity = 8192
;

-- ------------------------------------------------------------
-- topology_inference_results
-- ------------------------------------------------------------
CREATE TABLE event_intelligence.topology_inference_results (`inference_id` String, `use_case` String, `unknown_msn` String, `event_ts` String, `candidate_rank` UInt8, `candidate_feeder` String, `candidate_substation` String, `supporting_meter_count` UInt32, `cooccurrence_score` Float64, `distance_km` Float64, `proximity_score` Float64, `topology_score` Float64, `prediction` String, `confidence` Float64, `severity` String, `reason` String, `recommendation` String, `generated_at` String, `received_at` DateTime64(3) DEFAULT now64(3)) ENGINE = MergeTree ORDER BY (unknown_msn, event_ts, candidate_rank, received_at) SETTINGS index_granularity = 8192
;

-- ------------------------------------------------------------
-- anomaly_detection_results
-- ------------------------------------------------------------
CREATE TABLE event_intelligence.anomaly_detection_results (`msn` String, `window_start` String, `window_end` String, `event_count_15m` UInt64, `avg_voltage_15m` Nullable(Float64), `voltage_range_15m` Nullable(Float64), `power_failure_count_15m` UInt64, `use_case` String, `model_type` String, `sklearn_version` String, `model_contamination` Float64, `prediction` String, `confidence` Nullable(Float64), `anomaly_score` Nullable(Float64), `severity` String, `reason` String, `recommendation` String, `received_at` DateTime64(3) DEFAULT now64(3)) ENGINE = MergeTree ORDER BY (msn, window_start, received_at) SETTINGS index_granularity = 8192
;

-- ------------------------------------------------------------
-- processed_events_queue_v2
-- ------------------------------------------------------------
CREATE TABLE event_intelligence.processed_events_queue_v2 (`msn` Nullable(String), `msn_id` Nullable(Int64), `evnt_id` Nullable(Int32), `event_ts` Nullable(String), `log_seq_no` Nullable(Int64), `v_r` Nullable(Float64), `v_y` Nullable(Float64), `v_b` Nullable(Float64), `validation_status` String, `validation_reason` Nullable(String), `event_catalogue_match` Nullable(Bool), `event_tblrefid` Nullable(Int32), `event_name` Nullable(String), `event_state` Nullable(String), `isrestoration` Nullable(Bool), `priorityname` Nullable(String), `eventclassification_name` Nullable(String), `hierarchy_match` Nullable(Bool), `msn_normalized` Nullable(String), `dtr_name` Nullable(String), `dtr_network_code` Nullable(String), `feeder_name` Nullable(String), `substation_name` Nullable(String)) ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka-local:29092', kafka_topic_list = 'processed-smart-meter-events', kafka_group_name = 'clickhouse-processed-events-v1', kafka_format = 'JSONEachRow', kafka_num_consumers = 1
;

-- ------------------------------------------------------------
-- meter_features_15m_queue
-- ------------------------------------------------------------
CREATE TABLE event_intelligence.meter_features_15m_queue (`msn` String, `window_start` DateTime, `window_end` DateTime, `event_count_15m` UInt64, `avg_voltage_15m` Nullable(Float64), `voltage_range_15m` Nullable(Float64), `power_failure_count_15m` UInt64) ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka-local:29092', kafka_topic_list = 'meter-features-15m', kafka_group_name = 'clickhouse-meter-features-15m-v1', kafka_format = 'JSONEachRow', kafka_num_consumers = 1
;

-- ------------------------------------------------------------
-- outage_incidents_queue
-- ------------------------------------------------------------
CREATE TABLE event_intelligence.outage_incidents_queue (`incident_id` String, `msn` String, `event_name` String, `event_tblrefid` Int32, `incident_status` String, `start_ts` Nullable(String), `end_ts` Nullable(String), `duration_minutes` Nullable(Float64), `dtr_name` Nullable(String), `dtr_network_code` Nullable(String), `feeder_name` Nullable(String), `substation_name` Nullable(String)) ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka-local:29092', kafka_topic_list = 'outage-incidents', kafka_group_name = 'clickhouse-outage-incidents-v1', kafka_format = 'JSONEachRow', kafka_num_consumers = 1
;

-- ------------------------------------------------------------
-- topology_inference_results_queue
-- ------------------------------------------------------------
CREATE TABLE event_intelligence.topology_inference_results_queue (`use_case` String, `inference_id` String, `unknown_msn` String, `event_ts` String, `candidate_rank` UInt8, `candidate_feeder` String, `candidate_substation` String, `supporting_meter_count` UInt32, `cooccurrence_score` Float64, `distance_km` Float64, `proximity_score` Float64, `topology_score` Float64, `prediction` String, `confidence` Float64, `severity` String, `reason` String, `recommendation` String, `generated_at` String) ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka-local:29092', kafka_topic_list = 'topology-inference-results', kafka_group_name = 'clickhouse-topology-inference-v1', kafka_format = 'JSONEachRow', kafka_num_consumers = 1
;

-- ------------------------------------------------------------
-- anomaly_detection_results_queue
-- ------------------------------------------------------------
CREATE TABLE event_intelligence.anomaly_detection_results_queue (`msn` String, `window_start` String, `window_end` String, `event_count_15m` UInt64, `avg_voltage_15m` Nullable(Float64), `voltage_range_15m` Nullable(Float64), `power_failure_count_15m` UInt64, `use_case` String, `model_type` String, `sklearn_version` String, `model_contamination` Float64, `prediction` String, `confidence` Nullable(Float64), `anomaly_score` Nullable(Float64), `severity` String, `reason` String, `recommendation` String) ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka-local:29092', kafka_topic_list = 'anomaly-detection-results', kafka_group_name = 'clickhouse-anomaly-detection-v1', kafka_format = 'JSONEachRow', kafka_num_consumers = 1
;

-- ------------------------------------------------------------
-- processed_events_mv_v2
-- ------------------------------------------------------------
CREATE MATERIALIZED VIEW event_intelligence.processed_events_mv_v2 TO event_intelligence.processed_events (`msn` Nullable(String), `msn_id` Nullable(Int64), `evnt_id` Nullable(Int32), `event_ts` Nullable(String), `log_seq_no` Nullable(Int64), `v_r` Nullable(Float64), `v_y` Nullable(Float64), `v_b` Nullable(Float64), `validation_status` String, `validation_reason` Nullable(String), `event_catalogue_match` Nullable(Bool), `event_tblrefid` Nullable(Int32), `event_name` Nullable(String), `event_state` Nullable(String), `isrestoration` Nullable(Bool), `priorityname` Nullable(String), `eventclassification_name` Nullable(String), `hierarchy_match` Nullable(Bool), `msn_normalized` Nullable(String), `dtr_name` Nullable(String), `dtr_network_code` Nullable(String), `feeder_name` Nullable(String), `substation_name` Nullable(String)) AS SELECT msn, msn_id, evnt_id, event_ts, log_seq_no, v_r, v_y, v_b, validation_status, validation_reason, event_catalogue_match, event_tblrefid, event_name, event_state, isrestoration, priorityname, eventclassification_name, hierarchy_match, msn_normalized, dtr_name, dtr_network_code, feeder_name, substation_name FROM event_intelligence.processed_events_queue_v2
;

-- ------------------------------------------------------------
-- meter_features_15m_mv
-- ------------------------------------------------------------
CREATE MATERIALIZED VIEW event_intelligence.meter_features_15m_mv TO event_intelligence.meter_features_15m (`msn` String, `window_start` DateTime, `window_end` DateTime, `event_count_15m` UInt64, `avg_voltage_15m` Nullable(Float64), `voltage_range_15m` Nullable(Float64), `power_failure_count_15m` UInt64) AS SELECT msn, window_start, window_end, event_count_15m, avg_voltage_15m, voltage_range_15m, power_failure_count_15m FROM event_intelligence.meter_features_15m_queue
;

-- ------------------------------------------------------------
-- outage_incidents_mv
-- ------------------------------------------------------------
CREATE MATERIALIZED VIEW event_intelligence.outage_incidents_mv TO event_intelligence.outage_incident_events (`incident_id` String, `msn` String, `event_name` String, `event_tblrefid` Int32, `incident_status` String, `start_ts` Nullable(String), `end_ts` Nullable(String), `duration_minutes` Nullable(Float64), `dtr_name` Nullable(String), `dtr_network_code` Nullable(String), `feeder_name` Nullable(String), `substation_name` Nullable(String)) AS SELECT incident_id, msn, event_name, event_tblrefid, incident_status, start_ts, end_ts, duration_minutes, dtr_name, dtr_network_code, feeder_name, substation_name FROM event_intelligence.outage_incidents_queue
;

-- ------------------------------------------------------------
-- topology_inference_results_mv
-- ------------------------------------------------------------
CREATE MATERIALIZED VIEW event_intelligence.topology_inference_results_mv TO event_intelligence.topology_inference_results (`inference_id` String, `use_case` String, `unknown_msn` String, `event_ts` String, `candidate_rank` UInt8, `candidate_feeder` String, `candidate_substation` String, `supporting_meter_count` UInt32, `cooccurrence_score` Float64, `distance_km` Float64, `proximity_score` Float64, `topology_score` Float64, `prediction` String, `confidence` Float64, `severity` String, `reason` String, `recommendation` String, `generated_at` String, `received_at` DateTime64(3)) AS SELECT inference_id, use_case, unknown_msn, event_ts, candidate_rank, candidate_feeder, candidate_substation, supporting_meter_count, cooccurrence_score, distance_km, proximity_score, topology_score, prediction, confidence, severity, reason, recommendation, generated_at, now64(3) AS received_at FROM event_intelligence.topology_inference_results_queue
;

-- ------------------------------------------------------------
-- anomaly_detection_results_mv
-- ------------------------------------------------------------
CREATE MATERIALIZED VIEW event_intelligence.anomaly_detection_results_mv TO event_intelligence.anomaly_detection_results (`msn` String, `window_start` String, `window_end` String, `event_count_15m` UInt64, `avg_voltage_15m` Nullable(Float64), `voltage_range_15m` Nullable(Float64), `power_failure_count_15m` UInt64, `use_case` String, `model_type` String, `sklearn_version` String, `model_contamination` Float64, `prediction` String, `confidence` Nullable(Float64), `anomaly_score` Nullable(Float64), `severity` String, `reason` String, `recommendation` String, `received_at` DateTime64(3)) AS SELECT msn, window_start, window_end, event_count_15m, avg_voltage_15m, voltage_range_15m, power_failure_count_15m, use_case, model_type, sklearn_version, model_contamination, prediction, confidence, anomaly_score, severity, reason, recommendation, now64(3) AS received_at FROM event_intelligence.anomaly_detection_results_queue
;


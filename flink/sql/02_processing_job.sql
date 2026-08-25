CREATE TABLE raw_smart_meter_events (
  payload STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'smart-meter-events',
  'properties.bootstrap.servers' = 'kafka-local:29092',
  'properties.group.id' = 'event-intelligence-processor',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'raw'
);

CREATE VIEW parsed_smart_meter_events AS
SELECT
  JSON_VALUE(payload, '$.msn') AS msn,
  CAST(JSON_VALUE(payload, '$.msn_id') AS BIGINT) AS msn_id,
  CAST(JSON_VALUE(payload, '$.evnt_id') AS INT) AS evnt_id,
  JSON_VALUE(payload, '$.ts') AS event_ts,
  CAST(JSON_VALUE(payload, '$.log_seq_no') AS BIGINT) AS log_seq_no,
  CAST(JSON_VALUE(payload, '$.v_r') AS DOUBLE) AS v_r,
  CAST(JSON_VALUE(payload, '$.v_y') AS DOUBLE) AS v_y,
  CAST(JSON_VALUE(payload, '$.v_b') AS DOUBLE) AS v_b,
  payload AS raw_payload
FROM raw_smart_meter_events;

CREATE VIEW validated_smart_meter_events AS
SELECT
  *,
  CASE
    WHEN msn IS NULL OR TRIM(msn) = '' THEN 'FAIL'
    ELSE 'PASS'
  END AS validation_status,
  CASE
    WHEN msn IS NULL OR TRIM(msn) = '' THEN 'MISSING_MSN'
    ELSE NULL
  END AS validation_reason
FROM parsed_smart_meter_events;

CREATE TABLE processed_kafka_events (
  msn STRING,
  msn_id BIGINT,
  evnt_id INT,
  event_ts STRING,
  log_seq_no BIGINT,
  v_r DOUBLE,
  v_y DOUBLE,
  v_b DOUBLE,
  validation_status STRING,
  validation_reason STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'processed-smart-meter-events',
  'properties.bootstrap.servers' = 'kafka-local:29092',
  'format' = 'json'
);

SET 'pipeline.name' = 'smart-meter-processing';

INSERT INTO processed_kafka_events
SELECT
  msn,
  msn_id,
  evnt_id,
  event_ts,
  log_seq_no,
  v_r,
  v_y,
  v_b,
  validation_status,
  validation_reason
FROM validated_smart_meter_events;
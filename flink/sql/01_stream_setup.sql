CREATE TABLE raw_smart_meter_events (
  payload STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'smart-meter-events',
  'properties.bootstrap.servers' = 'kafka-local:29092',
  'properties.group.id' = 'flink-smoke-test',
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
CREATE TABLE event_catalogue (
    event_id INT,
    event_tblrefid INT,
    event_name STRING,
    event_description STRING,
    amr_event_id INT,
    amr_eventstatus INT,
    event_state STRING,
    isrestoration BOOLEAN,
    prioritytblrefid INT,
    priorityname STRING,
    eventclassification_tblrefid INT,
    eventclassification_name STRING,
    phase STRING,
    dlmsevents_tblrefid INT
) WITH (
    'connector' = 'filesystem',
    'path' = 'file:///tmp/event_catalogue.csv',
    'format' = 'csv',
    'csv.ignore-parse-errors' = 'true'
);

CREATE TABLE network_hierarchy (
    substation_name STRING,
    feeder_name STRING,
    dtr_name STRING,
    dtr_network_code STRING,
    msn STRING,
    msn_normalized STRING
) WITH (
    'connector' = 'filesystem',
    'path' = 'file:///tmp/network_hierarchy.csv',
    'format' = 'csv',
    'csv.ignore-parse-errors' = 'true'
);

CREATE TABLE processed_events_for_enrichment (
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
    'properties.group.id' = 'event-intelligence-enrichment',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

CREATE VIEW enriched_smart_meter_events AS
SELECT
    p.msn,
    p.msn_id,
    p.evnt_id,
    p.event_ts,
    p.log_seq_no,
    p.v_r,
    p.v_y,
    p.v_b,
    p.validation_status,
    p.validation_reason,

    c.event_name,
    c.event_description,
    c.event_tblrefid,
    c.event_state,
    c.priorityname,
    c.eventclassification_name,

    h.dtr_name,
    h.dtr_network_code,
    h.feeder_name,
    h.substation_name

FROM processed_events_for_enrichment AS p

LEFT JOIN event_catalogue AS c
    ON p.evnt_id = c.event_id

LEFT JOIN network_hierarchy AS h
    ON UPPER(TRIM(p.msn)) = h.msn_normalized;

CREATE TABLE enriched_kafka_events (
    msn STRING,
    msn_id BIGINT,
    evnt_id INT,
    event_ts STRING,
    log_seq_no BIGINT,
    v_r DOUBLE,
    v_y DOUBLE,
    v_b DOUBLE,
    validation_status STRING,
    validation_reason STRING,

    event_name STRING,
    event_description STRING,
    event_tblrefid INT,
    event_state STRING,
    priorityname STRING,
    eventclassification_name STRING,

    dtr_name STRING,
    dtr_network_code STRING,
    feeder_name STRING,
    substation_name STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'enriched-smart-meter-events',
    'properties.bootstrap.servers' = 'kafka-local:29092',
    'format' = 'json'
);

SET 'pipeline.name' = 'smart-meter-enrichment';

INSERT INTO enriched_kafka_events
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
    validation_reason,

    event_name,
    event_description,
    event_tblrefid,
    event_state,
    priorityname,
    eventclassification_name,

    dtr_name,
    dtr_network_code,
    feeder_name,
    substation_name

FROM enriched_smart_meter_events;
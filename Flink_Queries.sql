%flink.ssql(type=update)
DROP TABLE IF EXISTS smart_device_logs_stream;

CREATE TABLE smart_device_logs_stream (
   `id` VARCHAR(40),
   `operation_time` TIMESTAMP,
   `location_name` VARCHAR(128),
   `device_type` VARCHAR(128),
   `device_name` VARCHAR(128),
   `change_of_status` INT,
   `device_power_in_watts` INT,
   `session_id` VARCHAR(40),
   `arrival_time` TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL,
   `shard_id` VARCHAR(128) NOT NULL METADATA FROM 'shard-id' VIRTUAL,
   `sequence_number` VARCHAR(128) NOT NULL METADATA FROM 'sequence-number' VIRTUAL)
 WITH (
   'connector' = 'kinesis',
   'stream' = 'smart_device_kinesis_stream',
   'aws.region' = 'us-east-1',
   'scan.stream.initpos' = 'TRIM_HORIZON',
   'format' = 'json'
);




%flink.ssql(type=update)
-- select * from smart_device_logs_stream;

-- SELECT
--     operation_time,
--     CASE
--         WHEN change_of_status = 'on' THEN 1
--         WHEN change_of_status = 'off' THEN 0
--         ELSE -1  -- Handle other cases, using -1 for unknown status
--     END AS status_value
-- FROM
--     smart_device_logs_stream
-- WHERE
--     device_name = 'Bedroom Light';


-- select * from smart_device_logs_stream;

SELECT
  operation_date,
  device_name,
  SUM(avg_power_usage_in_watts) as avg_power_usage_in_watts
FROM
  (
    SELECT
      CAST(MAX(operation_time) AS DATE) as operation_date,
      device_name,
      CASE
        WHEN TIMESTAMPDIFF(SECOND, MIN(operation_time), MAX(operation_time)) > 0
        THEN (TIMESTAMPDIFF(SECOND, MIN(operation_time), MAX(operation_time))/ 3600.0 * MAX(device_power_in_watts))
        ELSE 0
      END AS avg_power_usage_in_watts
    --   (TIMESTAMPDIFF(SECOND, MIN(operation_time), MAX(operation_time))/ 3600.0 * MAX(device_power_in_watts)) AS avg_power_usage_in_watts
    from
      smart_device_logs_stream
    group by
      session_id, device_name
  ) as device_data group by device_name, operation_date;

--       SELECT
--     CAST(MAX(operation_time) AS DATE) AS operation_date,  -- Extracting date from timestamp
--     device_name,
--     (TIMESTAMPDIFF(SECOND, MIN(operation_time), MAX(operation_time)) / 3600.0 * MAX(device_power_in_watts)) AS avg_power_usage_in_watts  -- Calculating the average power usage
-- FROM
--     smart_device_logs_stream
-- GROUP BY
--     session_id, device_name;






%flink.ssql(type=update)
drop TABLE if exists smart_device_status_statistics;

CREATE TABLE smart_device_status_statistics (
        `id` VARCHAR(40),
        `operation_time` TIMESTAMP,
        `location_name` VARCHAR(128),
        `device_type` VARCHAR(128),
        `device_name` VARCHAR(128),
        `change_of_status` INT,
        `device_power_in_watts` INT,
        `session_id` VARCHAR(40)
    ) WITH (
  'connector' = 'elasticsearch-7',
  'hosts' = 'https://search-srm-smart-device-logs-search-vflzahymd323yyudgpfxwuqpum.us-east-1.es.amazonaws.com:443',
  'index' = 'smart_device_usage_sample_statistics',
  'username' = 'admin',
  'password' = 'Admin123*'
);


%flink.ssql(type=query)
INSERT INTO smart_device_status_statistics
SELECT
        id,
        operation_time,
        location_name,
        device_type,
        device_name,
        change_of_status,
        device_power_in_watts,
        session_id
FROM smart_device_logs_stream;
-- WHERE taxi_trips.pickupLatitude <> 0 AND taxi_trips.pickupLongitude <> 0 AND taxi_trips.dropoffLatitude <> 0 AND taxi_trips.dropoffLongitude <> 0;


%flink.ssql(type=update)
drop TABLE if exists smart_device_power_usage_statistics;

CREATE TABLE smart_device_power_usage_statistics (
        `operation_date` DATE,
        `device_name` VARCHAR(128),
        `avg_power_usage_in_watts` DECIMAL(15, 2)
    ) WITH (
  'connector' = 'elasticsearch-7',
  'hosts' = 'https://search-srm-smart-device-logs-search-vflzahymd323yyudgpfxwuqpum.us-east-1.es.amazonaws.com:443',
  'index' = 'smart_device_power_usage_sample_statistics',
  'username' = 'admin',
  'password' = 'Admin123*'
);



%flink.ssql(type=update)

INSERT INTO smart_device_power_usage_statistics
  SELECT
    operation_date,
    device_name,
    SUM(avg_power_usage_in_watts) as avg_power_usage_in_watts
  FROM
  (
    SELECT
      CAST(MAX(operation_time) AS DATE) as operation_date,
      device_name,
      CASE
        WHEN TIMESTAMPDIFF(SECOND, MIN(operation_time), MAX(operation_time)) > 0
        THEN (TIMESTAMPDIFF(SECOND, MIN(operation_time), MAX(operation_time))/ 3600.0 * MAX(device_power_in_watts))
        ELSE 0
      END AS avg_power_usage_in_watts
    --   (TIMESTAMPDIFF(SECOND, MIN(operation_time), MAX(operation_time))/ 3600.0 * MAX(device_power_in_watts)) AS avg_power_usage_in_watts
    from
      smart_device_logs_stream
    group by
      session_id, device_name
  ) as device_data group by device_name, operation_date;


%flink.ssql(type=update)
-- SELECT * FROM smart_device_power_usage_statistics;
SELECT * FROM smart_device_status_statistics;
-- SELECT * FROM smart_device_logs_stream;
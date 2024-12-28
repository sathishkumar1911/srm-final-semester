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
select * from smart_device_logs_stream
WHERE
    location_name = 'Chennai House'
    and device_type = 'Television'
    and device_name = 'Dining Area TV' ;

%flink.ssql(type=update)
select * from smart_device_logs_stream
WHERE
    location_name = 'Beach House'
    and device_type = 'Tube Light'
    and device_name = 'Hall Light' ;


%flink.ssql(type=update)
select * from smart_device_logs_stream;

  %flink.ssql(type=update)

  SELECT
    UUID() as id,
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
    from
      smart_device_logs_stream
      where device_name = 'Dining Area TV'
    group by
      session_id, device_name order by operation_date
  ) as device_data group by device_name, operation_date;

%flink.ssql(type=update)

  SELECT
    UUID() as id,
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
    from
      smart_device_logs_stream
    group by
      session_id, device_name order by operation_date
  ) as device_data group by device_name, operation_date;





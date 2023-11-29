SET 'state.checkpoints.dir' = 'file:///tmp/checkpoints/';
SET 'execution.checkpointing.interval' = '60000';

-- // source table
CREATE TABLE source_tbl(
  `id`      STRING,
  `value`   INT,
  `ts`      TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders',
  'properties.bootstrap.servers' = 'kafka-0:9092',
  'properties.group.id' = 'orders-source',
  'format' = 'json',
  'scan.startup.mode' = 'latest-offset'
);

--// Glue Catalog
CREATE CATALOG glue_catalog WITH (
  'type' = 'hive',
  'default-database' = 'default',
  'hive-conf-dir' = '/glue/confs/hive/conf'
);

CREATE DATABASE IF NOT EXISTS glue_catalog.demo 
  WITH ('hive.database.location-uri'= 's3://demo-ap-southeast-2/warehouse/');

SET table.sql-dialect=hive;

CREATE TABLE glue_catalog.demo.sink_tbl(
  `id`      STRING,
  `value`   INT,
  `ts`      TIMESTAMP(9)
) 
PARTITIONED BY (`year` STRING, `month` STRING, `date` STRING, `hour` STRING) 
STORED AS parquet 
-- LOCATION 's3://demo-ap-southeast-2/warehouse/order_sink'
TBLPROPERTIES (
  'partition.time-extractor.timestamp-pattern'='$year-$month-$date $hour:00:00',
  'sink.partition-commit.trigger'='partition-time',
  'sink.partition-commit.delay'='1 h',
  'sink.partition-commit.policy.kind'='metastore,success-file'
);

INSERT INTO TABLE glue_catalog.demo.sink_tbl
SELECT 
  `id`, 
  `value`, 
  `ts`,
  DATE_FORMAT(`ts`, 'yyyy') AS `year`,
  DATE_FORMAT(`ts`, 'MM') AS `month`,
  DATE_FORMAT(`ts`, 'dd') AS `date`,
  DATE_FORMAT(`ts`, 'HH') AS `hour`
FROM source_tbl;
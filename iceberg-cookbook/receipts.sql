--
-- // Basic
-- 

-- // query an iceberg table
show current namespace;
select count(*) from nyc_taxi_yellow;

-- // query table metadata
-- snapshots – known versions of the table, with summary metadata
-- manifests – manifest files in the current snapshot; these track data and delete files
-- data_files – data files in the current snapshot
-- delete_files – delete files in the current snapshot; these store deleted keys or file offsets

SELECT snapshot_id, committed_at, operation, summary['spark.app.id']
FROM examples.nyc_taxi_yellow.snapshots;

DESCRIBE examples.nyc_taxi_yellow.snapshots;
-- committed_at            timestamp                                   
-- snapshot_id             bigint                                      
-- parent_id               bigint                                      
-- operation               string                                      
-- manifest_list           string                                      
-- summary                 map<string,string> 

SELECT summary
FROM examples.nyc_taxi_yellow.snapshots limit 1;
-- {"added-data-files":"22","added-files-size":"99033595","added-records":"7667792","changed-partition-count":"15","partition-summaries-included":"true","partitions.pickup_time_month=2001-02":"added-data-files=1,added-records=1,added-files-size=5046","partitions.pickup_time_month=2003-01":"added-data-files=1,added-records=2,added-files-size=5634","partitions.pickup_time_month=2009-01":"added-data-files=1,added-records=72,added-files-size=7881","partitions.pickup_time_month=2018-11":"added-data-files=1,added-records=11,added-files-size=6001","partitions.pickup_time_month=2018-12":"added-data-files=1,added-records=47,added-files-size=7188","partitions.pickup_time_month=2019-01":"added-data-files=8,added-records=7535226,added-files-size=97421632","partitions.pickup_time_month=2019-02":"added-data-files=1,added-records=132409,added-files-size=1537029","partitions.pickup_time_month=2019-03":"added-data-files=1,added-records=5,added-files-size=5653","partitions.pickup_time_month=2019-04":"added-data-files=1,added-records=6,added-files-size=5723","partitions.pickup_time_month=2019-05":"added-data-files=1,added-records=1,added-files-size=5045","partitions.pickup_time_month=2019-06":"added-data-files=1,added-records=2,added-files-size=5471","partitions.pickup_time_month=2019-07":"added-data-files=1,added-records=6,added-files-size=5698","partitions.pickup_time_month=2019-08":"added-data-files=1,added-records=1,added-files-size=5045","partitions.pickup_time_month=2019-09":"added-data-files=1,added-records=1,added-files-size=5046","partitions.pickup_time_month=2088-01":"added-data-files=1,added-records=2,added-files-size=5503","spark.app.id":"local-1649956407299","total-data-files":"22","total-delete-files":"0","total-equality-deletes":"0","total-files-size":"99033595","total-position-deletes":"0","total-records":"7667792"}

DESCRIBE examples.nyc_taxi_yellow.history;
-- made_current_at         timestamp                                   
-- snapshot_id             bigint                                      
-- parent_id               bigint                                      
-- is_current_ancestor     boolean 

SELECT
     h.made_current_at,
     s.operation,
     h.snapshot_id,
     h.is_current_ancestor,
     s.summary['spark.app.id']
 FROM examples.nyc_taxi_yellow.history h
 JOIN examples.nyc_taxi_yellow.snapshots s
     ON h.snapshot_id = s.snapshot_id
 ORDER BY made_current_at;

-- // time travel to historical snapshots
-- Time travel queries are supported in Apache Spark and Trino 
  -- using the SQL clauses FOR VERSION AS OF and FOR TIMESTAMP AS OF

select count(*) from examples.nyc_taxi_yellow;

select snapshot_id
from examples.nyc_taxi_yellow.snapshots
order by committed_at limit 1;
-- 1678648647395174002

select count(*) from examples.nyc_taxi_yellow
for version as of 1678648647395174002;

SELECT * 
FROM examples.nyc_taxi_yellow.history;
-- 2022-09-29 16:30:59.707 7521683503157563610     4747211985173557225     true

SELECT count(*) FROM examples.nyc_taxi_yellow
FOR TIMESTAMP AS OF TIMESTAMP '2022-09-29 16:30:59.707';

--
-- // Data Engineering
-- 

-- // using hidden partitioning

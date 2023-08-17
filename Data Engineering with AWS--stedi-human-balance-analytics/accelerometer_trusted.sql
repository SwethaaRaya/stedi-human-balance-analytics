CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`accelerometer_trusted` (
  `user` string,
  `timeStamp` bigint,
  `x` double,
  `y` double,
  `z` double
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://stedi-human-balance-analytics/accelerometer_trusted/'
TBLPROPERTIES ('classification' = 'parquet');
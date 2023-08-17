CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`machine_learning_curated` (
  `timeStamp` bigint,
  `user` string,
  `serialNumber` string,
  `sensorReadingTime` bigint,
  `distanceFromObject` int,
  `x` double,
  `y` double,
  `z` double
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://stedi-human-balance-analytics/machine_learning_curated/'
TBLPROPERTIES ('classification' = 'parquet');
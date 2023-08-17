CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`customer_curated` (
  `serialNumber` string,
  `birthDay` string,
  `customerName` string,
  `email` string,
  `phone` string,
  `shareWithResearchAsOfDate` bigint,
  `registrationDate` bigint,
  `lastUpdateDate` bigint,
  `shareWithFriendsAsOfDate` bigint,
  `shareWithPublicAsOfDate` bigint
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://stedi-human-balance-analytics/customer_curated/'
TBLPROPERTIES ('classification' = 'parquet');
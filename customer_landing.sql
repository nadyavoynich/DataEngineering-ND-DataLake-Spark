CREATE EXTERNAL TABLE `customer_landing`(
  `customername` string COMMENT 'from deserializer',
  `email` string COMMENT 'from deserializer',
  `phone` string COMMENT 'from deserializer',
  `birthday` string COMMENT 'from deserializer',
  `serialnumber` string COMMENT 'from deserializer',
  `registrationdate` double COMMENT 'from deserializer',
  `lastupdatedate` double COMMENT 'from deserializer',
  `sharewithresearchasofdate` double COMMENT 'from deserializer',
  `sharewithpublicasofdate` double COMMENT 'from deserializer')
ROW FORMAT SERDE
  'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://whiterose-lake-house/customer/landing/'
TBLPROPERTIES (
  'TableType'='EXTERNAL_TABLE',
  'classification'='json')
CREATE EXTERNAL TABLE `step_trainer_landing`(
  `sensorreadingtime` double COMMENT 'from deserializer',
  `serialnumber` string COMMENT 'from deserializer',
  `distancefromobject` double COMMENT 'from deserializer')
ROW FORMAT SERDE
  'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://whiterose-lake-house/step_trainer/landing/'
TBLPROPERTIES (
  'TableType'='EXTERNAL_TABLE',
  'classification'='json')
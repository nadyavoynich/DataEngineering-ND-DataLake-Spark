import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1699349998498 = glueContext.create_dynamic_frame.from_catalog(
    database="whiterose",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1699349998498",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1699274419752 = glueContext.create_dynamic_frame.from_catalog(
    database="whiterose",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1699274419752",
)

# Script generated for node Join
SqlQuery1508 = """
SELECT DISTINCT 
    a.timestamp,
    a.user,
    a.x, 
    a.y, 
    a.z, 
    s.sensorreadingtime, 
    s.serialnumber, 
    s.distancefromobject
FROM 
    accelerometer_trusted a
JOIN 
    step_trainer_trusted s
ON 
    a.timestamp = s.sensorreadingtime;
"""
Join_node1699278354718 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1508,
    mapping={
        "accelerometer_trusted": AccelerometerTrusted_node1699274419752,
        "step_trainer_trusted": StepTrainerTrusted_node1699349998498,
    },
    transformation_ctx="Join_node1699278354718",
)

# Script generated for node Select Distinct
SqlQuery1509 = """
select distinct * from myDataSource
"""
SelectDistinct_node1699350361964 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1509,
    mapping={"myDataSource": Join_node1699278354718},
    transformation_ctx="SelectDistinct_node1699350361964",
)

# Script generated for node Drop Fields
DropFields_node1699353585130 = DropFields.apply(
    frame=SelectDistinct_node1699350361964,
    paths=["user"],
    transformation_ctx="DropFields_node1699353585130",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1699275431408 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1699353585130,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://whiterose-lake-house/machine_learning_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="MachineLearningCurated_node1699275431408",
)

job.commit()

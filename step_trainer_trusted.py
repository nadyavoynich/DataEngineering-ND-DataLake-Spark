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

# Script generated for node Customer Curated
CustomerCurated_node1699260933571 = glueContext.create_dynamic_frame.from_catalog(
    database="whiterose",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node1699260933571",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1699261190248 = glueContext.create_dynamic_frame.from_catalog(
    database="whiterose",
    table_name="step_trainer_landing",
    transformation_ctx="StepTrainerLanding_node1699261190248",
)

# Script generated for node Join
SqlQuery1732 = """
select step_trainer_landing.sensorreadingtime, step_trainer_landing.serialnumber, step_trainer_landing.distancefromobject
from step_trainer_landing, customer_curated
where step_trainer_landing.serialnumber = customer_curated.serialnumber
"""
Join_node1699265823094 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1732,
    mapping={
        "customer_curated": CustomerCurated_node1699260933571,
        "step_trainer_landing": StepTrainerLanding_node1699261190248,
    },
    transformation_ctx="Join_node1699265823094",
)

# Script generated for node Select Distinct
SqlQuery1733 = """
select distinct * from myDataSource

"""
SelectDistinct_node1699269260355 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1733,
    mapping={"myDataSource": Join_node1699265823094},
    transformation_ctx="SelectDistinct_node1699269260355",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1699269351676 = glueContext.getSink(
    path="s3://whiterose-lake-house/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrusted_node1699269351676",
)
StepTrainerTrusted_node1699269351676.setCatalogInfo(
    catalogDatabase="whiterose", catalogTableName="step_trainer_trusted"
)
StepTrainerTrusted_node1699269351676.setFormat("json")
StepTrainerTrusted_node1699269351676.writeFrame(SelectDistinct_node1699269260355)
job.commit()

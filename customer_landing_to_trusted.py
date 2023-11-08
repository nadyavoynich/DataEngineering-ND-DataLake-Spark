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

# Script generated for node Landing Customer Zone
LandingCustomerZone_node1698089543781 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://whiterose-lake-house/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="LandingCustomerZone_node1698089543781",
)

# Script generated for node SQL Query
SqlQuery1809 = """
select * from myDataSource
where sharewithresearchasofdate is not null;
"""
SQLQuery_node1699004829590 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1809,
    mapping={"myDataSource": LandingCustomerZone_node1698089543781},
    transformation_ctx="SQLQuery_node1699004829590",
)

# Script generated for node Trusted Customer Zone
TrustedCustomerZone_node1698089945465 = glueContext.getSink(
    path="s3://whiterose-lake-house/customer/trusted/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="TrustedCustomerZone_node1698089945465",
)
TrustedCustomerZone_node1698089945465.setCatalogInfo(
    catalogDatabase="whiterose", catalogTableName="customer_trusted"
)
TrustedCustomerZone_node1698089945465.setFormat("json")
TrustedCustomerZone_node1698089945465.writeFrame(SQLQuery_node1699004829590)
job.commit()

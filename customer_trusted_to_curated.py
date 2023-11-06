import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1698829248874 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://whiterose-lake-house/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1698829248874",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1698829183283 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://whiterose-lake-house/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1698829183283",
)

# Script generated for node Customer Privacy Filter
CustomerPrivacyFilter_node1698829501219 = Join.apply(
    frame1=CustomerTrusted_node1698829183283,
    frame2=AccelerometerTrusted_node1698829248874,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="CustomerPrivacyFilter_node1698829501219",
)

# Script generated for node Drop Fields
DropFields_node1699024055129 = DropFields.apply(
    frame=CustomerPrivacyFilter_node1698829501219,
    paths=["z", "user", "y", "x", "timestamp"],
    transformation_ctx="DropFields_node1699024055129",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1699184068653 = DynamicFrame.fromDF(
    DropFields_node1699024055129.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1699184068653",
)

# Script generated for node Customer Curated Zone
CustomerCuratedZone_node1698829985944 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1699184068653,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://whiterose-lake-house/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCuratedZone_node1698829985944",
)

job.commit()

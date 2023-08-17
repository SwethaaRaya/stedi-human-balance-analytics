import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer_landing
accelerometer_landing_node1692187100256 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-human-balance-analytics/accelerometer_landing /"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_landing_node1692187100256",
)

# Script generated for node customer_trusted
customer_trusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://stedi-human-balance-analytics/customer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted_node1",
)

# Script generated for node Join
Join_node1692187196392 = Join.apply(
    frame1=customer_trusted_node1,
    frame2=accelerometer_landing_node1692187100256,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1692187196392",
)

# Script generated for node Drop Fields
DropFields_node1692187215040 = DropFields.apply(
    frame=Join_node1692187196392,
    paths=["user", "timeStamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1692187215040",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1692187215040,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://stedi-human-balance-analytics/customer_curated/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="S3bucket_node3",
)

job.commit()

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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://stedi-human-balance-analytics/accelerometer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_trusted_node1",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1692248126703 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://stedi-human-balance-analytics/step_trainer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_trusted_node1692248126703",
)

# Script generated for node Join
Join_node1692248183327 = Join.apply(
    frame1=accelerometer_trusted_node1,
    frame2=step_trainer_trusted_node1692248126703,
    keys1=["timeStamp"],
    keys2=["sensorReadingTime"],
    transformation_ctx="Join_node1692248183327",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1692248183327,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://stedi-human-balance-analytics/machine_learning_curated/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="S3bucket_node3",
)

job.commit()

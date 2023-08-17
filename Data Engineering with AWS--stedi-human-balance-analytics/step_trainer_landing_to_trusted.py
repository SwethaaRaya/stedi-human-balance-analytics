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

# Script generated for node customer_curated
customer_curated_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://stedi-human-balance-analytics/customer_curated/"],
        "recurse": True,
    },
    transformation_ctx="customer_curated_node1",
)

# Script generated for node step_trainer_landing
step_trainer_landing_node1692246200321 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-human-balance-analytics/step_trainer_landing/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_landing_node1692246200321",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1692246655192 = ApplyMapping.apply(
    frame=customer_curated_node1,
    mappings=[
        ("serialNumber", "string", "right_serialNumber", "string"),
        ("birthDay", "string", "birthDay", "string"),
        ("shareWithPublicAsOfDate", "bigint", "shareWithPublicAsOfDate", "bigint"),
        ("shareWithResearchAsOfDate", "bigint", "shareWithResearchAsOfDate", "bigint"),
        ("registrationDate", "bigint", "registrationDate", "bigint"),
        ("customerName", "string", "customerName", "string"),
        ("shareWithFriendsAsOfDate", "bigint", "shareWithFriendsAsOfDate", "bigint"),
        ("email", "string", "email", "string"),
        ("lastUpdateDate", "bigint", "lastUpdateDate", "bigint"),
        ("phone", "string", "phone", "string"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1692246655192",
)

# Script generated for node Join
Join_node1692246274692 = Join.apply(
    frame1=RenamedkeysforJoin_node1692246655192,
    frame2=step_trainer_landing_node1692246200321,
    keys1=["right_serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1692246274692",
)

# Script generated for node Drop Fields
DropFields_node1692246524290 = DropFields.apply(
    frame=Join_node1692246274692,
    paths=[
        "right_serialNumber",
        "birthDay",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "shareWithFriendsAsOfDate",
        "email",
        "lastUpdateDate",
        "phone",
    ],
    transformation_ctx="DropFields_node1692246524290",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1692246524290,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://stedi-human-balance-analytics/step_trainer_trusted/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="S3bucket_node3",
)

job.commit()

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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1655805658688 = glueContext.create_dynamic_frame.from_catalog(
    database="db_youtube_cleansed",
    table_name="raw_statistics",
    transformation_ctx="AWSGlueDataCatalog_node1655805658688",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1655805578259 = glueContext.create_dynamic_frame.from_catalog(
    database="db_youtube_cleansed",
    table_name="cleansed_raw_statistics_reference_data",
    transformation_ctx="AWSGlueDataCatalog_node1655805578259",
)

# Script generated for node Join
Join_node1655805685141 = Join.apply(
    frame1=AWSGlueDataCatalog_node1655805578259,
    frame2=AWSGlueDataCatalog_node1655805658688,
    keys1=["id"],
    keys2=["category_id"],
    transformation_ctx="Join_node1655805685141",
)

# Script generated for node Amazon S3
AmazonS3_node1655805762778 = glueContext.getSink(
    path="s3://bigdata-on-youtube-analytics-euwest1-14317622-dev/youtube/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["region", "id"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1655805762778",
)
AmazonS3_node1655805762778.setCatalogInfo(
    catalogDatabase="db_youtube_analytics",
    catalogTableName="rpt_youtube_statistics_categories",
)
AmazonS3_node1655805762778.setFormat("glueparquet")
AmazonS3_node1655805762778.writeFrame(Join_node1655805685141)
job.commit()

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

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="sales_data", table_name="raw", transformation_ctx="S3bucket_node1"
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("id", "long", "id", "long"),
        ("region", "string", "region", "string"),
        ("country", "string", "country", "string"),
        ("item_type", "string", "item_type", "string"),
        ("sales_channel", "string", "sales_channel", "string"),
        ("order_priority", "string", "order_priority", "string"),
        ("order_date", "string", "order_date", "string"),
        ("order_id", "long", "order_id", "long"),
        ("ship_date", "string", "ship_date", "string"),
        ("units_sold", "long", "units_sold", "long"),
        ("unit_price", "double", "unit_price", "double"),
        ("unit_cost", "double", "unit_cost", "double"),
        ("total_revenue", "double", "total_revenue", "double"),
        ("total_cost", "double", "total_cost", "double"),
        ("total_profit", "double", "total_profit", "double"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node2,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://learn1-glue-bucket-20220412/sales1-data/procesado/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="S3bucket_node3",
)

job.commit()

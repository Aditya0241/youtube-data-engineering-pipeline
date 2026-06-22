import sys
from pyspark.sql.functions import input_file_name, regexp_extract, col
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import ApplyMapping, ResolveChoice, DropNullFields
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# --- Glue job bootstrap ---
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --- Config ---
RAW_S3_PATH = "s3://youtube-analysis-data/youtube/raw_statistics/"
CLEANSED_S3_PATH = "s3://youtube-analysis-data/youtube/cleansed/raw_statistics/"
regions = ["ca", "de", "fr", "gb", "in", "jp", "kr", "mx", "ru", "us"]

# --- Robust CSV Read (multiline + permissive) ---
df = (
    spark.read
        .option("header", "true")
        .option("sep", ",")
        .option("quote", '"')
        .option("escape", "\\")              # backslash escape
        .option("multiLine", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .csv(RAW_S3_PATH)
)

# --- Add region from S3 folder name (region=us, region=jp, etc.) ---
df = df.withColumn("source_file", input_file_name())
df = df.withColumn("region", regexp_extract(col("source_file"), r"region=([^/]+)", 1))

# Optional: filter to regions list
df = df.filter(col("region").isin(regions))

# Only drop bad rows if Spark created the column
if "_corrupt_record" in df.columns:
    df = df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")

# Convert to DynamicFrame for Glue transforms
datasource0 = DynamicFrame.fromDF(df.drop("source_file"), glueContext, "datasource0")

# --- Apply mapping (types) ---
applymapping1 = ApplyMapping.apply(
    frame=datasource0,
    mappings=[
        ("video_id", "string", "video_id", "string"),
        ("trending_date", "string", "trending_date", "string"),
        ("title", "string", "title", "string"),
        ("channel_title", "string", "channel_title", "string"),
        ("category_id", "long", "category_id", "long"),
        ("publish_time", "string", "publish_time", "string"),
        ("tags", "string", "tags", "string"),
        ("views", "long", "views", "long"),
        ("likes", "long", "likes", "long"),
        ("dislikes", "long", "dislikes", "long"),
        ("comment_count", "long", "comment_count", "long"),
        ("thumbnail_link", "string", "thumbnail_link", "string"),
        ("comments_disabled", "boolean", "comments_disabled", "boolean"),
        ("ratings_disabled", "boolean", "ratings_disabled", "boolean"),
        ("video_error_or_removed", "boolean", "video_error_or_removed", "boolean"),
        ("description", "string", "description", "string"),
        ("region", "string", "region", "string"),
    ],
    transformation_ctx="applymapping1"
)

resolvechoice2 = ResolveChoice.apply(
    frame=applymapping1,
    choice="make_struct",
    transformation_ctx="resolvechoice2"
)

dropnullfields3 = DropNullFields.apply(
    frame=resolvechoice2,
    transformation_ctx="dropnullfields3"
)

# --- Write Parquet partitioned by region ---
glueContext.write_dynamic_frame.from_options(
    frame=dropnullfields3,
    connection_type="s3",
    connection_options={"path": CLEANSED_S3_PATH, "partitionKeys": ["region"]},
    format="parquet",
    transformation_ctx="datasink4"
)

job.commit()

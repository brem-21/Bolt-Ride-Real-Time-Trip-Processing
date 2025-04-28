import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, sum, count, avg, max, min
from awsglue.dynamicframe import DynamicFrame

# Initialize contexts and job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'DYNAMO_TABLE_NAME', 'S3_BUCKET_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

dynamodb_table_name = args['DYNAMO_TABLE_NAME']
s3_bucket_name = args['S3_BUCKET_NAME']

# Read from DynamoDB
dynamo_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="dynamodb",
    connection_options={
        "dynamodb.input.tableName": dynamodb_table_name,
        "dynamodb.throughput.read.percent": "0.5"
    }
)

s3_prefix = f"s3://{s3_bucket_name}/KPIs"
# Convert DynamicFrame to DataFrame for transformations
trips = dynamo_dyf.toDF()

# Filter for completed trips
completed_trips = trips.filter(col("dropoff_datetime").isNotNull())

# Create daily KPIs from all trips
all_trips_daily_kpis = trips.groupBy("pickup_datetime").agg(
    sum("fare_amount").alias("total_fare"),
    count("fare_amount").alias("count_trips"),
    avg("fare_amount").alias("average_fare"),
    max("fare_amount").alias("max_fare"),
    min("fare_amount").alias("min_fare")
)

# Optimize completed trips with repartitioning
completed_trips = completed_trips.repartition(2, col("pickup_datetime"))

# Create daily KPIs from completed trips
completed_trips_daily_kpis = completed_trips.groupBy("dropoff_datetime").agg(
    sum("fare_amount").alias("total_fare"),
    count("fare_amount").alias("count_trips"),
    avg("fare_amount").alias("average_fare"),
    max("fare_amount").alias("max_fare"),
    min("fare_amount").alias("min_fare")
)

# Rename column for clarity
completed_trips_daily_kpis = completed_trips_daily_kpis.withColumnRenamed("dropoff_datetime", "DAY")

# Convert back to DynamicFrames - THIS IS THE FIXED PART
completed_trips_dyf = DynamicFrame.fromDF(completed_trips, glueContext, "completed_trips")
all_trips_daily_kpis_dyf = DynamicFrame.fromDF(all_trips_daily_kpis, glueContext, "all_trips_daily_kpis")
completed_trips_daily_kpis_dyf = DynamicFrame.fromDF(completed_trips_daily_kpis, glueContext, "completed_trips_daily_kpis")

# Write each result to S3
# 1. Write the original data from DynamoDB
glueContext.write_dynamic_frame.from_options(
    frame=dynamo_dyf,
    connection_type="s3",
    connection_options={
        "path": f"{s3_prefix}/dynamo-data"
    },
    format="parquet"
)

# 2. Write completed trips
glueContext.write_dynamic_frame.from_options(
    frame=completed_trips_dyf,
    connection_type="s3",
    connection_options={
        "path": f"{s3_prefix}/completed_trips"
    },
    format="parquet"
)

# 3. Write all trips daily KPIs
glueContext.write_dynamic_frame.from_options(
    frame=all_trips_daily_kpis_dyf,
    connection_type="s3",
    connection_options={
        "path": f"{s3_prefix}/all_trips_daily_kpis"
    },
    format="parquet"
)

# 4. Write completed trips daily KPIs
glueContext.write_dynamic_frame.from_options(
    frame=completed_trips_daily_kpis_dyf,
    connection_type="s3",
    connection_options={
        "path": f"{s3_prefix}/completed_trips_daily_kpis"
    },
    format="parquet"
)

# Commit the job
job.commit()
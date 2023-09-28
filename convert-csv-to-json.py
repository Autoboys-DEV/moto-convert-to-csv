import sys
import os
import boto3
import json
from io import StringIO
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import input_file_name

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Read the data from S3
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "multiline": True,
    },
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://moto-pricefile-dump/inputFiles/"]},
    transformation_ctx="S3bucket_node1",
)

# Convert DynamicFrame to DataFrame
df = S3bucket_node1.toDF()

# Add a new column with the input filename
df_with_filename = df.withColumn("input_file_name", input_file_name())

# Extract unique file names
file_names = df_with_filename.select('input_file_name').distinct().collect()

s3_client = boto3.client('s3')

for file_name in file_names:
    current_df = df_with_filename.filter(df_with_filename["input_file_name"] == file_name.input_file_name)
    
    # Extract the base file name without path and extension
    base_file_name = os.path.basename(file_name.input_file_name).replace('.csv', '')
    
    # Convert to JSON
    json_rows = [json.dumps(row.asDict()) for row in current_df.drop("input_file_name").collect()]
    json_data = "\n".join(json_rows)
    
    # Upload to S3 using boto3
    s3_client.put_object(
        Bucket='moto-pricefile-dump',
        Key=f'convertedFiles/{base_file_name}.json',
        Body=json_data
    )

job.commit()
import os
import logging
import boto3
from botocore.exceptions import ClientError, BotoCoreError
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Fetch environment variables
start_data = os.getenv("trip_start")
end_data = os.getenv("trip_end")
access_key = os.getenv("Access_key_ID")
secret_key = os.getenv("Secret_access_key")
bucket_name = os.getenv("bucket_name")
region = os.getenv("region")
s3_prefix = "raw-data/"


# Check if essential variables are None
if not bucket_name:
    logger.error("Bucket name is not defined in the environment variables.")
    raise ValueError("Bucket name is required.")
if not start_data:
    logger.error("Start data path is not defined in the environment variables.")
    raise ValueError("Start data path is required.")
if not access_key or not secret_key:
    logger.error("AWS credentials (Access key or Secret key) are missing.")
    raise ValueError("AWS Access key ID and Secret access key are required.")

# Initialize S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name=region
)

def upload_file_to_s3(s3_client, file_path, bucket_name, s3_key):
    """Upload a single file to S3."""
    if file_path is None:
        logger.error("File path is None. Please provide a valid file path.")
        raise ValueError("File path cannot be None.")
    
    if not os.path.exists(file_path):
        logger.error(f"File {file_path} does not exist.")
        raise FileNotFoundError(f"File {file_path} does not exist.")
    
    try:
        s3_client.upload_file(file_path, bucket_name, s3_key)
        logger.info(f"Successfully uploaded {file_path} to s3://{bucket_name}/{s3_key}")
    except ClientError as e:
        logger.error(f"Failed to upload {file_path} to S3: {str(e)}")
        raise

# Upload files to S3
try:
    s3_key = f"{s3_prefix}{os.path.basename(start_data)}"
    upload_file_to_s3(s3, start_data, bucket_name, s3_key)
except (ValueError, FileNotFoundError, ClientError) as e:
    logger.error(f"Error uploading file: {str(e)}")


try:
    s3_key = f"{s3_prefix}{os.path.basename(end_data)}"
    upload_file_to_s3(s3, start_data, bucket_name, s3_key)
except (ValueError, FileNotFoundError, ClientError) as e:
    logger.error(f"Error uploading file: {str(e)}")
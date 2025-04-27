import os
import json
import boto3
import logging
from botocore.exceptions import ClientError

# Initialize clients for S3 and Kinesis
s3 = boto3.client('s3')
kinesis = boto3.client('kinesis')

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

bucket_name = os.environ['BUCKET_NAME']
stream_name = os.environ['STREAM_NAME']

def lambda_handler(event, context):
    object_key = os.environ['OBJECT_KEY']

    try:
        # Fetch the S3 object
        response = s3.get_object(Bucket=bucket_name, Key=object_key)

        file_content = response['Body'].read().decode('utf-8').splitlines()

        batch_size = 500  # Respect Kinesis put_records limit
        for i in range(0, len(file_content), batch_size):
            batch = file_content[i:i + batch_size]
            send_to_kinesis(batch)

        return {
            'statusCode': 200,
            'body': json.dumps('Data sent to Kinesis successfully')
        }

    except ClientError as e:
        logger.error(f"Error fetching the object {object_key} from S3: {e}")
        raise e

def send_to_kinesis(records):
    kinesis_records = []
    header = 'trip_id,pickup_location_id,dropoff_location_id,vendor_id,pickup_datetime,estimated_dropoff_datetime,estimated_fare_amount'

    for record in records:
        # Skip header row
        if record.strip() == header:
            logger.info("Skipping header row")
            continue

        try:
            # Split CSV line and extract fields
            fields = record.split(',')
            if len(fields) != 7:
                logger.warning(f"Invalid CSV record: {record}, skipping")
                continue

            # Map fields to a dictionary with correct field names
            record_data = {
                'trip_id': fields[0],
                'pickup_location_id': int(fields[1]),
                'dropoff_location_id': int(fields[2]),
                'vendor_id': int(fields[3]),
                'pickup_datetime': fields[4],
                'estimated_dropoff_datetime': fields[5],
                'estimated_fare_amount': float(fields[6])
            }
            pickup_datetime = record_data['pickup_datetime']

        except (ValueError, IndexError) as e:
            logger.warning(f"Error parsing CSV record: {record}, error: {e}, skipping")
            continue

        kinesis_records.append({
            'Data': json.dumps

(record_data),
            'PartitionKey': str(pickup_datetime)  # Use pickup_datetime as partition key
        })

    if not kinesis_records:
        logger.warning("No valid records to send to Kinesis")
        return

    try:
        # Put records to Kinesis stream
        response = kinesis.put_records(
            Records=kinesis_records,
            StreamName=stream_name
        )

        # Log the result
        logger.info(f"Sent {len(kinesis_records)} records to Kinesis")
        return response
    except ClientError as e:
        logger.error(f"Error sending records to Kinesis: {e}")
        raise e
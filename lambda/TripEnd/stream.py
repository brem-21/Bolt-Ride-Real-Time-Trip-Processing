import os
import json
import boto3
import logging
from botocore.exceptions import ClientError

#Initialize logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

#Inititalize boto3 clients
s3 = boto3.client('s3')
kinesis = boto3.client('kinesis')

bucket_name = os.environ['BUCKET_NAME']
stream_name = os.environ['STREAM_NAME']

def lambda_handler(event, context):
    object_key = os.environ['OBJECT_KEY']

    try:
        response = s3.get_object(Bucket=bucket_name, Key=object_key)  # Fixed typo here
        file_content = response['Body'].read().decode('utf-8').splitlines()

        batch_size = 500 
        for i in range(0, len(file_content), batch_size):
            batch = file_content[i:i+batch_size]
            send_to_kinesis(batch)

        return {
            'statusCode': 200,
            'body': json.dumps('Data sent to Kinesis successfully')
        }

    except ClientError as e:
        logger.error(f"Error fetching the object {object_key} from s3: {e}")
        raise e

def send_to_kinesis(records):
    kinesis_records = []
    header = 'dropoff_datetime,rate_code,passenger_count,trip_distance,fare_amount,tip_amount,payment_type,trip_type,trip_id'

    for record in records:
        #skip header
        if record.strip() == header:
            logger.info("Skipping header row")
            continue

        try:
            # split CSV line and extract fields
            fields = record.split(',')
            if len(fields) != 9:
                logger.warning(f"Skipping invalid record: {record}")
                continue

            #Map fields to a dictionary with correct field names
            record_data = {
                'dropoff_datetime': fields[0],
                'rate_code': fields[1],
                'passenger_count': fields[2],
                'trip_distance': fields[3],
                'fare_amount': fields[4],
                'tip_amount': fields[5],
                'payment_type': fields[6],
                'trip_type': fields[7],
                'trip_id': fields[8]
            }

            trip_id = record_data['trip_id']

        except Exception as e:
            logger.error(f"Error parsing CSV record: {record}. Error: {e}")
            continue
        
        kinesis_records.append({
            'Data': json.dumps(record_data),
            'PartitionKey': trip_id
        })

    if not kinesis_records:
        logger.info("No valid records to send to Kinesis")
        return

    try:
        # Put records to kinesis stream
        response = kinesis.put_records(
            Records=kinesis_records,
            StreamName=stream_name
        )

        # Log the result
        logger.info(f"Sent {len(kinesis_records)} records to kinesis")
        return response
        
    except ClientError as e:
        logger.error(f"Error sending records to kinesis: {e}")
        raise e
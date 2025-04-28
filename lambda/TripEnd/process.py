import json
import boto3
import logging
import base64
import os
from botocore.exceptions import ClientError

# Initialize logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize boto3 client for DynamoDB
dynamodb = boto3.client('dynamodb')

# Environment variables
table_name = os.environ.get('TABLE_NAME')

def is_valid_value(value, attribute_type):
    """Validate if the value is suitable for DynamoDB update."""
    if value is None:
        return False
    if attribute_type == 'S':
        return isinstance(value, str) and value.strip() != ''
    if attribute_type == 'N':
        return isinstance(value, (int, float)) and value != 0
    return True

def lambda_handler(event, context):
    """
    Lambda function to process Kinesis stream records and update DynamoDB table with valid values.
    """
    if not table_name:
        logger.error("TABLE_NAME environment variable is not set")
        raise ValueError("TABLE_NAME environment variable is not set")

    processed_records = 0
    failed_records = 0

    try:
        logger.info(f"Received {len(event['Records'])} Kinesis records")

        for record in event['Records']:
            try:
                # Decode Kinesis data (base64-encoded JSON)
                kinesis_data = json.loads(base64.b64decode(record['kinesis']['data']).decode('utf-8'))
                trip_id = kinesis_data.get('trip_id')
                logger.info(f"Processing record with trip_id: {trip_id}")

                # Validate required fields
                if not trip_id:
                    logger.warning("Skipping record with missing trip_id")
                    failed_records += 1
                    continue

                # Define expected attributes and their types
                expected_attributes = {
                    'dropoff_datetime': 'S',
                    'rate_code': 'S',
                    'passenger_count': 'N',
                    'trip_distance': 'N',
                    'fare_amount': 'N',
                    'tip_amount': 'N',
                    'payment_type': 'S',
                    'trip_type': 'S'
                }

                # Build update expression dynamically
                update_expression_parts = []
                expression_attribute_values = {}
                for attr, attr_type in expected_attributes.items():
                    value = kinesis_data.get(attr)
                    if is_valid_value(value, attr_type):
                        update_expression_parts.append(f"{attr} = :{attr}")
                        expression_attribute_values[f":{attr}"] = (
                            {'N': str(value)} if attr_type == 'N' else {'S': str(value)}
                        )

                if not update_expression_parts:
                    logger.warning(f"No valid fields to update for trip_id: {trip_id}")
                    failed_records += 1
                    continue

                update_expression = "SET " + ", ".join(update_expression_parts)
                logger.info(f"Updating trip_id: {trip_id} with values: {expression_attribute_values}")

                # Update DynamoDB
                response = dynamodb.update_item(
                    TableName=table_name,
                    Key={'trip_id': {'S': trip_id}},
                    UpdateExpression=update_expression,
                    ExpressionAttributeValues=expression_attribute_values,
                    ReturnValues="UPDATED_NEW"
                )
                logger.info(f"Successfully updated item with trip_id: {trip_id}")
                processed_records += 1

            except ClientError as e:
                logger.error(f"DynamoDB error for trip_id {kinesis_data.get('trip_id', 'unknown')}: {e}")
                failed_records += 1
                continue
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode JSON for record: {record['kinesis']['data']}: {e}")
                failed_records += 1
                continue
            except Exception as e:
                logger.error(f"Unexpected error processing record: {e}")
                failed_records += 1
                continue

        logger.info(f"Processed {processed_records} records successfully, {failed_records} failed")
        return {
            'statusCode': 200,
            'body': json.dumps(f"Processed {processed_records} records, {failed_records} failed")
        }

    except Exception as e:
        logger.error(f"Critical error processing Kinesis records: {e}")
        raise e
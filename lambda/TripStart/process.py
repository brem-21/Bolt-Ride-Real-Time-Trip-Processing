import json
import boto3
import base64
from decimal import Decimal
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

dynamodb = boto3.client('dynamodb')  # Use client for batch_write_item
table_name = 'TaxiData'

def lambda_handler(event, context):
    processed_records = 0
    batch_size = 25  # DynamoDB batch_write_item limit
    valid_fields = {
        'trip_id', 'pickup_location_id', 'dropoff_location_id', 'vendor_id',
        'pickup_datetime', 'estimated_dropoff_datetime', 'estimated_fare_amount', 'pickup_time'
    }
    items_to_write = []

    for record in event['Records']:
        try:
            # Decode Kinesis record payload
            payload = json.loads(base64.b64decode(record['kinesis']['data']).decode('utf-8'))
            logger.info(f"Processing record with trip_id: {payload.get('trip_id')}")

            # Process payload
            payload = process_trip_start(payload)

            # Filter to valid fields
            payload = {k: v for k, v in payload.items() if k in valid_fields}

            # Convert estimated_fare_amount to Decimal for DynamoDB
            if payload.get('estimated_fare_amount') is not None:
                try:
                    payload['estimated_fare_amount'] = Decimal(str(float(payload['estimated_fare_amount'])))
                except (ValueError, TypeError) as e:
                    logger.error(f"Invalid estimated_fare_amount: {payload['estimated_fare_amount']}")
                    continue

            # Convert payload to DynamoDB item format
            item = {
                k: {'S': str(v)} if isinstance(v, (str, datetime)) else
                   {'N': str(v)} if isinstance(v, (int, float, Decimal)) else
                   {'NULL': True} if v is None else {'S': str(v)}
                for k, v in payload.items()
            }

            # Add to batch
            items_to_write.append({'PutRequest': {'Item': item}})
            processed_records += 1

            # Write batch when it reaches batch_size or at the end
            if len(items_to_write) >= batch_size:
                write_batch(items_to_write)
                items_to_write = []  # Clear batch after writing

        except Exception as e:
            logger.error(f"Error processing record: {e}, raw_record: {record}")
            continue

    # Write any remaining items
    if items_to_write:
        write_batch(items_to_write)

    return {
        'statusCode': 200,
        'body': json.dumps(f'Successfully processed {processed_records} records')
    }

def write_batch(items):
    """Write a batch of items to DynamoDB with retry for unprocessed items."""
    if not items:
        return

    max_retries = 3
    retry_delay = 0.1  # Initial delay in seconds
    attempt = 0

    while items and attempt < max_retries:
        try:
            response = dynamodb.batch_write_item(
                RequestItems={
                    table_name: items
                }
            )

            # Check for unprocessed items
            unprocessed_items = response.get('UnprocessedItems', {}).get(table_name, [])
            if not unprocessed_items:
                logger.info(f"Successfully wrote {len(items)} items to DynamoDB")
                return

            # Update items to retry
            items = unprocessed_items
            logger.warning(f"Retrying {len(items)} unprocessed items")

        except Exception as e:
            logger.error(f"Error writing batch to DynamoDB: {e}")
            attempt += 1
            if attempt < max_retries:
                import time
                time.sleep(retry_delay * (2 ** attempt))  # Exponential backoff
            continue

    if items:
        logger.error(f"Failed to write {len(items)} items after {max_retries} attempts: {items}")

def process_trip_start(payload):
    """Process the trip data to ensure schema compliance."""
    try:
        if payload.get('pickup_datetime'):
            # Parse pickup_datetime (expected: '2024-05-25 23:34:00' or '2024-05-25')
            try:
                pickup_dt = datetime.strptime(payload['pickup_datetime'], '%Y-%m-%d %H:%M:%S')
                payload['pickup_time'] = pickup_dt.strftime('%H:%M:%S')
                payload['pickup_datetime'] = pickup_dt.date().isoformat()
            except ValueError:
                # Handle case where pickup_datetime is already a date
                pickup_dt = datetime.strptime(payload['pickup_datetime'], '%Y-%m-%d')
                payload['pickup_time'] = None
                payload['pickup_datetime'] = pickup_dt.date().isoformat()
        else:
            payload['pickup_time'] = None

        # Round estimated_fare_amount to 2 decimal places
        if payload.get('estimated_fare_amount') is not None:
            payload['estimated_fare_amount'] = round(float(payload['estimated_fare_amount']), 2)

        # Ensure estimated_dropoff_datetime is a timestamp
        if payload.get('estimated_dropoff_datetime'):
            dropoff_dt = datetime.strptime(payload['estimated_dropoff_datetime'], '%Y-%m-%d %H:%M:%S')
            payload['estimated_dropoff_datetime'] = dropoff_dt.strftime('%Y-%m-%d %H:%M:%S')

    except (ValueError, KeyError) as e:
        logger.error(f"Error processing trip start: {e}, payload: {payload}")

    return payload
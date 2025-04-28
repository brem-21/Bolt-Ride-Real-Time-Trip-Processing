# Bolt-Ride Real-Time Trip Processing

A production-grade, event-driven architecture for processing ride-hailing data using AWS services. This system handles real-time trip events, performs analytics, and generates business insights through automated pipelines.

## Table of Contents
- [Architecture Overview](#architecture-overview)
- [Technical Stack](#technical-stack)
- [Detailed Components](#detailed-components)
- [Data Flow](#data-flow)
- [Setup and Installation](#setup-and-installation)
- [Development Guide](#development-guide)
- [Monitoring and Maintenance](#monitoring-and-maintenance)
- [Testing Strategy](#testing-strategy)

## Architecture Overview

### System Components
```
[S3 Raw Data] → [Lambda Triggers] → [Kinesis Streams] → [Processing Lambdas] → [DynamoDB]
                                                                ↓
[Step Functions] → [Glue ETL Jobs] → [S3 Analytics] → [KPI Dashboard]
```

### Key Services Integration
- **S3**: Stores raw trip data and processed analytics
- **Lambda**: Processes trip events in real-time
- **Kinesis**: Handles data streaming
- **DynamoDB**: Primary data storage
- **Glue**: ETL processing
- **Step Functions**: Workflow orchestration
- **SNS**: Notification system

## Technical Stack

### AWS Services
- AWS Lambda (Python 3.8+)
- Amazon Kinesis Data Streams
- Amazon DynamoDB
- AWS Glue (PySpark)
- AWS Step Functions
- Amazon S3
- Amazon SNS

### Development Tools
- Python 3.8+
- PySpark
- pytest
- black (code formatting)
- AWS SDK (boto3)

## Detailed Components

### 1. Trip Start Processing
```python
# Lambda processes trip start events
{
    "trip_id": "string",
    "pickup_location_id": "integer",
    "dropoff_location_id": "integer",
    "vendor_id": "integer",
    "pickup_datetime": "timestamp",
    "estimated_dropoff_datetime": "timestamp",
    "estimated_fare_amount": "float"
}
```

### 2. Trip End Processing
```python
# Lambda processes trip completion events
{
    "trip_id": "string",
    "dropoff_datetime": "timestamp",
    "rate_code": "string",
    "passenger_count": "integer",
    "trip_distance": "float",
    "fare_amount": "float",
    "tip_amount": "float",
    "payment_type": "string",
    "trip_type": "string"
}
```

### 3. Analytics Pipeline
The Glue job performs the following transformations:
- Data cleaning and validation
- Trip completion status tracking
- Daily KPI calculations
- Data partitioning and optimization

## Data Flow

### 1. Ingestion Layer
- Raw data uploaded to S3
- Lambda functions trigger on S3 events
- Data streamed through Kinesis
- Real-time processing and validation

### 2. Processing Layer
- Lambda functions process stream data
- DynamoDB updates with validated data
- Error handling and retry mechanisms
- SNS notifications for status updates

### 3. Analytics Layer
- Glue jobs extract data from DynamoDB
- PySpark transformations for KPI calculation
- Results stored in optimized Parquet format
- Partitioned by date for efficient querying

## Setup and Installation

### Prerequisites
- AWS Account with appropriate permissions
- Python 3.8+ installed
- AWS CLI configured

### Environment Setup
1. Clone the repository:
```bash
git clone <repository-url>
cd bolt-ride-processing
```

2. Create virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
.\venv\Scripts\activate   # Windows
```

3. Install dependencies:
```bash
make install
```

### AWS Configuration
1. Create required AWS resources:
```bash
# Example CloudFormation command
aws cloudformation create-stack \
    --stack-name bolt-ride-stack \
    --template-body file://infrastructure/template.yaml
```

2. Configure environment variables:
```bash
# .env file
Access_key_ID=your_access_key
Secret_access_key=your_secret_key
bucket_name=your_bucket_name
region=your_region
```

## Development Guide

### Code Structure
```
project/
├── lambda/
│   ├── TripStart/
│   │   ├── Stream.py      # Kinesis stream processor
│   │   └── process.py     # Main processing logic
│   └── TripEnd/
│       ├── stream.py
│       └── process.py
├── scripts/
│   ├── Glue/
│   │   └── glue_script.py # ETL processing
│   ├── step/
│   │   └── step_function.json
│   ├── data.py
│   └── test_data.py
├── notebooks/
│   ├── trips.ipynb        # Analysis notebooks
│   └── s3_upload.ipynb
└── tests/
```

### Local Development
1. Run tests:
```bash
make test
```

2. Format code:
```bash
make format
```

3. Run full check:
```bash
make all
```

## Monitoring and Maintenance

### CloudWatch Metrics
- Lambda execution metrics
- Kinesis stream throughput
- Glue job performance
- Step Function execution status

### Error Handling
- Comprehensive error catching in Lambda functions
- Dead Letter Queues for failed processes
- SNS notifications for critical errors
- Retry mechanisms with exponential backoff

### Performance Optimization
- DynamoDB read/write capacity monitoring
- Kinesis shard management
- Glue job tuning
- S3 partition optimization

## Testing Strategy

### Unit Tests
```python
# Example test case
def test_process_trip_start():
    sample_data = {
        "trip_id": "test_123",
        "pickup_datetime": "2024-01-01 10:00:00"
    }
    result = process_trip_start(sample_data)
    assert result["pickup_time"] == "10:00:00"
```

### Integration Tests
- End-to-end workflow testing
- AWS service integration verification
- Error handling validation

### Performance Tests
- Load testing for Lambda functions
- Stress testing for Kinesis streams
- Throughput testing for DynamoDB

## License

GNU General Public License v3.0

## Contributing

1. Fork the repository
2. Create feature branch
3. Commit changes
4. Push to branch
5. Create Pull Request

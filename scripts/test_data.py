import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DateType,
    TimestampType,
    DoubleType,
)
from pyspark.sql import Row
from datetime import datetime, date
from data import (
    create_spark_session,
    load_trip_data,
    process_trip_start,
    process_trip_end,
    filter_completed_trips,
    calculate_daily_kpis,
)


@pytest.fixture(scope="session")
def spark_session():
    """Fixture to create a Spark session for tests."""
    spark = (
        SparkSession.builder.appName("TestTaxiTrips").master("local[2]").getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def trip_start_schema():
    """Schema for trip_start DataFrame."""
    return StructType(
        [
            StructField("trip_id", StringType(), True),
            StructField("pickup_location_id", IntegerType(), True),
            StructField("dropoff_location_id", IntegerType(), True),
            StructField("vendor_id", IntegerType(), True),
            StructField("pickup_datetime", TimestampType(), True),
            StructField("estimated_dropoff_datetime", TimestampType(), True),
            StructField("estimated_fare_amount", DoubleType(), True),
        ]
    )


@pytest.fixture
def trip_end_schema():
    """Schema for trip_end DataFrame."""
    return StructType(
        [
            StructField("trip_id", StringType(), True),
            StructField("dropoff_datetime", TimestampType(), True),
            StructField("rate_code", DoubleType(), True),
            StructField("passenger_count", DoubleType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("tip_amount", DoubleType(), True),
            StructField("payment_type", DoubleType(), True),
            StructField("trip_type", DoubleType(), True),
        ]
    )


@pytest.fixture
def trip_start_df(spark_session, trip_start_schema):
    """Fixture to create sample trip_start DataFrame."""
    data = [
        Row(
            trip_id="1",
            pickup_location_id=100,
            dropoff_location_id=200,
            vendor_id=1,
            pickup_datetime=datetime(2023, 1, 1, 10, 0),
            estimated_dropoff_datetime=datetime(2023, 1, 1, 10, 30),
            estimated_fare_amount=15.50,
        ),
        Row(
            trip_id="2",
            pickup_location_id=101,
            dropoff_location_id=201,
            vendor_id=2,
            pickup_datetime=datetime(2023, 1, 1, 12, 0),
            estimated_dropoff_datetime=datetime(2023, 1, 1, 12, 30),
            estimated_fare_amount=20.75,
        ),
    ]
    return spark_session.createDataFrame(data, schema=trip_start_schema)


@pytest.fixture
def trip_end_df(spark_session, trip_end_schema):
    """Fixture to create sample trip_end DataFrame."""
    data = [
        Row(
            trip_id="1",
            dropoff_datetime=datetime(2023, 1, 1, 10, 30),
            rate_code=1.0,
            passenger_count=2.0,
            trip_distance=3.5,
            fare_amount=16.00,
            tip_amount=2.50,
            payment_type=1.0,
            trip_type=1.0,
        ),
        Row(
            trip_id="2",
            dropoff_datetime=None,
            rate_code=None,
            passenger_count=None,
            trip_distance=None,
            fare_amount=None,
            tip_amount=None,
            payment_type=None,
            trip_type=None,
        ),
    ]
    return spark_session.createDataFrame(data, schema=trip_end_schema)


def test_create_spark_session():
    """Test Spark session creation."""
    spark = create_spark_session()
    assert isinstance(spark, SparkSession)
    assert spark.sparkContext.appName == "TaxiTrips"
    spark.stop()


def test_load_trip_data(spark_session, tmp_path):
    """Test loading CSV data with trip_start schema."""
    # Create a temporary CSV file
    data = [
        {
            "trip_id": "1",
            "pickup_location_id": 100,
            "dropoff_location_id": 200,
            "vendor_id": 1,
            "pickup_datetime": "2023-01-01 10:00:00",
            "estimated_dropoff_datetime": "2023-01-01 10:30:00",
            "estimated_fare_amount": 15.50,
        },
        {
            "trip_id": "2",
            "pickup_location_id": 101,
            "dropoff_location_id": 201,
            "vendor_id": 2,
            "pickup_datetime": "2023-01-01 12:00:00",
            "estimated_dropoff_datetime": "2023-01-01 12:30:00",
            "estimated_fare_amount": 20.75,
        },
    ]
    import pandas as pd

    df = pd.DataFrame(data)
    csv_path = tmp_path / "test_trip_start.csv"
    df.to_csv(csv_path, index=False)

    # Load the data
    result = load_trip_data(spark_session, str(csv_path))

    assert result.count() == 2
    expected_columns = [
        "trip_id",
        "pickup_location_id",
        "dropoff_location_id",
        "vendor_id",
        "pickup_datetime",
        "estimated_dropoff_datetime",
        "estimated_fare_amount",
    ]
    assert all(col in result.columns for col in expected_columns)


def test_process_trip_start(trip_start_df):
    """Test processing of trip_start DataFrame."""
    result = process_trip_start(trip_start_df)

    # Check schema
    expected_columns = [
        "trip_id",
        "pickup_location_id",
        "dropoff_location_id",
        "vendor_id",
        "pickup_datetime",
        "estimated_dropoff_datetime",
        "estimated_fare_amount",
        "pickup_time",
    ]
    assert all(col in result.columns for col in expected_columns)

    # Check data transformations
    row = result.collect()[0]
    assert row.pickup_time == "10:00:00"
    assert row.estimated_fare_amount == 15.50
    assert isinstance(row.pickup_datetime, date)


def test_process_trip_end(trip_end_df):
    """Test processing of trip_end DataFrame."""
    result = process_trip_end(trip_end_df)

    # Check schema
    expected_columns = [
        "trip_id",
        "dropoff_datetime",
        "rate_code",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "tip_amount",
        "payment_type",
        "trip_type",
        "dropoff_time",
    ]
    assert all(col in result.columns for col in expected_columns)

    # Check data transformations
    row = result.collect()[0]
    assert row.dropoff_time == "10:30:00"
    assert row.fare_amount == 16.00
    assert isinstance(row.dropoff_datetime, date)


def test_filter_completed_trips(trip_start_df, trip_end_df):
    """Test filtering of completed trips."""
    # Join trip_start and trip_end to create completed_trips
    trips = trip_start_df.join(trip_end_df, on="trip_id", how="inner")
    result = filter_completed_trips(trips)

    assert result.count() == 1
    row = result.collect()[0]
    assert row.trip_id == "1"
    assert row.dropoff_datetime is not None


def test_calculate_daily_kpis(spark_session):
    """Test calculation of daily KPIs."""
    # Create a sample completed_trips DataFrame
    completed_schema = StructType(
        [
            StructField("trip_id", StringType(), True),
            StructField("pickup_datetime", DateType(), True),
            StructField("dropoff_datetime", DateType(), True),
            StructField("fare_amount", DoubleType(), True),
            # Include other fields as needed
        ]
    )
    data = [
        Row(
            trip_id="1",
            pickup_datetime=date(2023, 1, 1),
            dropoff_datetime=date(2023, 1, 1),
            fare_amount=16.00,
        ),
        Row(
            trip_id="2",
            pickup_datetime=date(2023, 1, 1),
            dropoff_datetime=date(2023, 1, 1),
            fare_amount=20.00,
        ),
    ]
    df = spark_session.createDataFrame(data, schema=completed_schema)

    result = calculate_daily_kpis(df)

    row = result.collect()[0]
    assert row.total_fare == 36.00
    assert row.count_trips == 2
    assert row.average_fare == 18.00
    assert row.max_fare == 20.00
    assert row.min_fare == 16.00

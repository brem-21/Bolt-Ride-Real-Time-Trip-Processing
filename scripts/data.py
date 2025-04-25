import os
import sys
import logging
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, round, sum, count, avg, max, min

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session():
    """Create a Spark session."""
    return SparkSession.builder.appName("TaxiTrips").getOrCreate()


def load_trip_data(spark, file_path):
    """Load the CSV data into a DataFrame."""
    return (
        spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
    )


def process_trip_start(trip_start):
    """Process the trip_start DataFrame."""
    trip_start = trip_start.withColumn(
        "pickup_time", date_format("pickup_datetime", "HH:mm:ss")
    )
    trip_start = trip_start.withColumn(
        "pickup_datetime", col("pickup_datetime").cast("date")
    )
    trip_start = trip_start.withColumn(
        "estimated_fare_amount", round(col("estimated_fare_amount"), 2)
    )
    return trip_start


def process_trip_end(trip_end):
    """Process the trip_end DataFrame."""
    trip_end = trip_end.withColumn(
        "dropoff_time", date_format("dropoff_datetime", "HH:mm:ss")
    )
    trip_end = trip_end.withColumn(
        "dropoff_datetime", col("dropoff_datetime").cast("date")
    )
    trip_end = trip_end.withColumn("fare_amount", round(col("fare_amount"), 2))
    return trip_end


def filter_completed_trips(trips):
    """Filter completed trips (where dropoff_datetime is not null)."""
    return trips.filter(col("dropoff_datetime").isNotNull())


def calculate_daily_kpis(trips):
    """Calculate the daily KPIs for completed trips."""
    return trips.groupBy("pickup_datetime").agg(
        sum("fare_amount").alias("total_fare"),
        count("fare_amount").alias("count_trips"),
        avg("fare_amount").alias("average_fare"),
        max("fare_amount").alias("max_fare"),
        min("fare_amount").alias("min_fare"),
    )


def main():
    # Create a Spark session
    spark = create_spark_session()

    # Load data
    trip_start = load_trip_data(spark, "../data/trip_start.csv")
    trip_end = load_trip_data(spark, "../data/trip_end.csv")

    # Process the trip_start and trip_end data
    trip_start = process_trip_start(trip_start)
    trip_end = process_trip_end(trip_end)

    # Show processed data for debugging
    trip_start.show()
    trip_end.show()

    # Join the trip_start and trip_end data on 'trip_id'
    trips = trip_start.join(trip_end, on="trip_id", how="inner")
    trips.show()

    # Filter completed trips
    completed_trips = filter_completed_trips(trips)
    completed_trips.show()

    # Repartition completed trips and calculate daily KPIs
    completed_trips = completed_trips.repartition(2, col("dropoff_datetime"))
    daily_kpis = calculate_daily_kpis(completed_trips)

    # Show daily KPIs
    daily_kpis.show()


if __name__ == "__main__":
    main()

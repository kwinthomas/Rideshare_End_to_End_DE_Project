from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

RIDE_SCHEMA = StructType([
    StructField("ride_id", StringType()),
    StructField("confirmation_number", StringType()),
    StructField("passenger_id", StringType()),
    StructField("driver_id", StringType()),
    StructField("vehicle_id", StringType()),
    StructField("vehicle_type_id", IntegerType()),
    StructField("vehicle_make_id", IntegerType()),
    StructField("payment_method_id", IntegerType()),
    StructField("ride_status_id", IntegerType()),
    StructField("pickup_city_id", IntegerType()),
    StructField("dropoff_city_id", IntegerType()),
    StructField("cancellation_reason_id", IntegerType()),
    StructField("passenger_name", StringType()),
    StructField("passenger_email", StringType()),
    StructField("passenger_phone", StringType()),
    StructField("driver_name", StringType()),
    StructField("driver_rating", DoubleType()),
    StructField("driver_phone", StringType()),
    StructField("driver_license", StringType()),
    StructField("vehicle_model", StringType()),
    StructField("vehicle_color", StringType()),
    StructField("license_plate", StringType()),
    StructField("pickup_address", StringType()),
    StructField("pickup_latitude", DoubleType()),
    StructField("pickup_longitude", DoubleType()),
    StructField("dropoff_address", StringType()),
    StructField("dropoff_latitude", DoubleType()),
    StructField("dropoff_longitude", DoubleType()),
    StructField("distance_miles", DoubleType()),
    StructField("duration_minutes", IntegerType()),
    StructField("booking_timestamp", StringType()),
    StructField("pickup_timestamp", StringType()),
    StructField("dropoff_timestamp", StringType()),
    StructField("base_fare", DoubleType()),
    StructField("distance_fare", DoubleType()),
    StructField("time_fare", DoubleType()),
    StructField("surge_multiplier", DoubleType()),
    StructField("subtotal", DoubleType()),
    StructField("tip_amount", DoubleType()),
    StructField("total_fare", DoubleType()),
    StructField("rating", IntegerType()),
])

RIDE_COLUMNS = [f.name for f in RIDE_SCHEMA.fields]

TYPED_COLUMNS = [
    col("ride_id"),
    col("confirmation_number"),
    col("passenger_id"),
    col("driver_id"),
    col("vehicle_id"),
    col("vehicle_type_id"),
    col("vehicle_make_id"),
    col("payment_method_id"),
    col("ride_status_id"),
    col("pickup_city_id"),
    col("dropoff_city_id"),
    col("cancellation_reason_id"),
    col("passenger_name"),
    col("passenger_email"),
    col("passenger_phone"),
    col("driver_name"),
    col("driver_rating"),
    col("driver_phone"),
    col("driver_license"),
    col("vehicle_model"),
    col("vehicle_color"),
    col("license_plate"),
    col("pickup_address"),
    col("pickup_latitude"),
    col("pickup_longitude"),
    col("dropoff_address"),
    col("dropoff_latitude"),
    col("dropoff_longitude"),
    col("distance_miles"),
    col("duration_minutes"),
    to_timestamp("booking_timestamp").alias("booking_timestamp"),
    to_timestamp("pickup_timestamp").alias("pickup_timestamp"),
    to_timestamp("dropoff_timestamp").alias("dropoff_timestamp"),
    col("base_fare"),
    col("distance_fare"),
    col("time_fare"),
    col("surge_multiplier"),
    col("subtotal"),
    col("tip_amount"),
    col("total_fare"),
    col("rating"),
]


dp.create_streaming_table(
    name="rideshare.silver.staging_rides",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
    },
    expect_all_or_drop={
        "valid_ride_id": "ride_id IS NOT NULL",
        "valid_booking_time": "booking_timestamp IS NOT NULL",
    },
    expect_all={
        "plausible_surge": "surge_multiplier BETWEEN 1.0 AND 3.0",
        "plausible_rating": "rating IS NULL OR rating BETWEEN 1 AND 5",
        "non_negative_fare": "total_fare >= 0",
    },
)


@dp.append_flow(
    target="rideshare.silver.staging_rides",
    name="flow_stream_rides",
    comment="Live rides from Event Hub.",
)
def flow_stream_rides():
    return (
        spark.readStream.table("rideshare.bronze.rides_raw")
        .withColumn("payload", from_json(col("ride_payload"), RIDE_SCHEMA))
        .select("payload.*", "kafka_enqueued_at", "_ingested_at")
        .withWatermark("kafka_enqueued_at", "10 minutes")
        .dropDuplicatesWithinWatermark(["ride_id"])
        .select(*TYPED_COLUMNS, lit("eventhub").alias("_source_system"),
                col("_ingested_at"))
    )


@dp.append_flow(
    target="rideshare.silver.staging_rides",
    name="flow_bulk_rides",
    comment="Historical rides backfilled from the ADF file load.",
)
def flow_bulk_rides():
    return (
        spark.readStream.table("rideshare.bronze.bulk_rides")
        .select(*RIDE_COLUMNS, "_ingested_at")
        .select(*TYPED_COLUMNS, lit("bulk_file").alias("_source_system"),
                col("_ingested_at"))
    )
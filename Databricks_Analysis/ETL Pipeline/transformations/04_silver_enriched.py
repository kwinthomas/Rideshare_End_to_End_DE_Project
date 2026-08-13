from pyspark import pipelines as dp
from pyspark.sql.functions import *


def _mapping(table, columns):
    """Static read of a bronze mapping table, lineage columns stripped."""
    return spark.read.table(f"rideshare.silver.{table}").select(*columns)


@dp.table(
    name="rideshare.silver.rides_enriched",
    comment="One big table: every ride, fully described. Source of truth for the gold layer.",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
    }
)
@dp.expect_all_or_drop({
    "valid_ride_id": "ride_id IS NOT NULL",
    "positive_distance": "distance_miles > 0",
    "non_negative_total": "total_fare >= 0",
    "coherent_timestamps": (
        "dropoff_timestamp IS NULL OR dropoff_timestamp >= pickup_timestamp"
    ),
})
@dp.expect_all({
    "fare_reconciles": (
        "abs(subtotal - round((base_fare + distance_fare + time_fare) "
        "* surge_multiplier, 2)) < 0.05"
    ),
    "cancelled_has_reason": (
        "ride_status_id = 1 OR cancellation_reason_id BETWEEN 1 AND 3"
    ),
    "pickup_in_range": "pickup_latitude BETWEEN -90 AND 90",
})
def rides_enriched():
    rides = spark.readStream.table("rideshare.silver.staging_rides")

    cities = _mapping("ref_cities", ["city_id", "city", "state", "region"])
    pickup_city = cities.select(
        col("city_id").alias("pu_city_id"),
        col("city").alias("pickup_city"),
        col("state").alias("pickup_state"),
        col("region").alias("pickup_region"),
    )
    dropoff_city = cities.select(
        col("city_id").alias("do_city_id"),
        col("city").alias("dropoff_city"),
        col("state").alias("dropoff_state"),
        col("region").alias("dropoff_region"),
    )

    vehicle_types = _mapping(
        "ref_vehicle_types",
        ["vehicle_type_id", "vehicle_type", "description", "base_rate", "per_mile", "per_minute"],
    ).withColumnRenamed("description", "vehicle_type_description")

    vehicle_makes = _mapping("ref_vehicle_makes", ["vehicle_make_id", "vehicle_make"])
    payment_methods = _mapping(
        "ref_payment_methods", ["payment_method_id", "payment_method", "is_card", "requires_auth"]
    )
    ride_statuses = _mapping("ref_ride_statuses", ["ride_status_id", "ride_status", "is_completed"])
    cancellations = _mapping(
        "ref_cancellation_reasons", ["cancellation_reason_id", "cancellation_reason"]
    )

    return (
        rides
        .join(pickup_city, rides.pickup_city_id == pickup_city.pu_city_id, "left")
        .join(dropoff_city, rides.dropoff_city_id == dropoff_city.do_city_id, "left")
        .join(vehicle_types, "vehicle_type_id", "left")
        .join(vehicle_makes, "vehicle_make_id", "left")
        .join(payment_methods, "payment_method_id", "left")
        .join(ride_statuses, "ride_status_id", "left")
        .join(cancellations, "cancellation_reason_id", "left")
        .drop("pu_city_id", "do_city_id")
        .withColumn("ride_date", to_date("pickup_timestamp"))
        .withColumn("pickup_hour", hour("pickup_timestamp"))
        .withColumn("pickup_day_of_week", date_format("pickup_timestamp", "EEEE"))
        .withColumn(
            "is_weekend",
            dayofweek("pickup_timestamp").isin(1, 7),
        )
        .withColumn("duration_hours", round(col("duration_minutes") / 60, 3))
        .withColumn(
            "avg_speed_mph",
            when(
                col("duration_minutes") > 0,
                round(col("distance_miles") / (col("duration_minutes") / 60), 2),
            ),
        )
        .withColumn(
            "fare_per_mile",
            when(
                col("distance_miles") > 0,
                round(col("total_fare") / col("distance_miles"), 2),
            ),
        )
        .withColumn(
            "tip_percentage",
            when(col("subtotal") > 0, round(100 * col("tip_amount") / col("subtotal"), 2)),
        )
        .withColumn("is_surged", col("surge_multiplier") > 1.0)
        .withColumn("is_intercity", col("pickup_city_id") != col("dropoff_city_id"))
        .withColumn(
            "distance_band",
            when(col("distance_miles") < 3, "Short")
             .when(col("distance_miles") < 10, "Medium")
             .when(col("distance_miles") < 25, "Long")
             .otherwise("Very long"),
        )
        .withColumn("_silver_processed_at", current_timestamp())
    )
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql.functions import *

REFERENCE_TABLES = {
    "ref_cities": {
        "source": "map_cities",
        "key": "city_id",
        "columns": {
            "city_id": "int",
            "city": "string",
            "state": "string",
            "region": "string"
        }
    },
    "ref_vehicle_types": {
        "source": "map_vehicle_types",
        "key": "vehicle_type_id",
        "columns": {
            "vehicle_type_id": "int",
            "vehicle_type": "string",
            "description": "string",
            "base_rate": "double",
            "per_mile": "double",
            "per_minute": "double"
        }
    },
    "ref_vehicle_makes": {
        "source": "map_vehicle_makes",
        "key": "vehicle_make_id",
        "columns": {
            "vehicle_make_id": "int",  
            "vehicle_make": "string"
        }
    },
    "ref_payment_methods": {
        "source": "map_payment_methods",
        "key": "payment_method_id",
        "columns": {
            "payment_method_id": "int",
            "payment_method": "string",
            "is_card": "boolean",
            "requires_auth": "boolean"
        }
    },
    "ref_ride_statuses": {
        "source": "map_ride_statuses",
        "key": "ride_status_id",
        "columns": {
            "ride_status_id": "int",
            "ride_status": "string",
            "is_completed": "boolean"
        }
    },
    "ref_cancellation_reasons": {
        "source": "map_cancellation_reasons",
        "key": "cancellation_reason_id",
        "columns": {
            "cancellation_reason_id": "int",
            "cancellation_reason": "string"
        }
    },
}


def make_reference_view(name, config):
    key = config["key"]

    @dp.materialized_view(
        name=f"rideshare.silver.{name}",
        table_properties={
            "quality": "silver",
            "delta.enableChangeDataFeed": "true",
        },
    )
    @dp.expect_or_fail("key_not_null", f"{key} IS NOT NULL")
    @dp.expect_or_fail("key_is_unique", f"_key_occurrences = 1")
    def my_view():
        source = spark.read.table(f"rideshare.bronze.{config['source']}")

        latest = Window.partitionBy(key).orderBy(
            col("_source_modified_at").desc(),
            col("_ingested_at").desc(),
        )

        deduped = (
            source
            .withColumn("_rn", row_number().over(latest))
            .filter(col("_rn") == 1)
            .drop("_rn")
        )

        typed = deduped.select(
            *[col(c).cast(t).alias(c) for c, t in config["columns"].items()],
            col("_source_file"),
            col("_ingested_at").alias("_bronze_ingested_at"),
        )

        return (
            typed
            .withColumn(
                "_key_occurrences",
                count("*").over(Window.partitionBy(key)),
            )
            .withColumn("_silver_processed_at", current_timestamp())
        )

    return my_view


for table_name, table_config in REFERENCE_TABLES.items():
    make_reference_view(table_name, table_config)
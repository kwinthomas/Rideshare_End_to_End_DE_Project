from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, expr

RAW_ROOT = "abfss://basecontainer@rideshareadls.dfs.core.windows.net/raw"

SOURCE_FILES = {
    "bulk_rides": "bulk_rides",
    "map_cities": "map_cities",
    "map_cancellation_reasons": "map_cancellation_reasons",
    "map_payment_methods": "map_payment_methods",
    "map_ride_statuses": "map_ride_statuses",
    "map_vehicle_makes": "map_vehicle_makes",
    "map_vehicle_types": "map_vehicle_types",
}

AUTOLOADER_OPTIONS = {
    "cloudFiles.format": "json",
    #"multiLine": "true",
    "cloudFiles.inferColumnTypes": "true",
    "cloudFiles.schemaEvolutionMode": "addNewColumns",
    "rescuedDataColumn": "_rescued_data",
}


def make_bronze_table(table_name, folder):
    @dp.table(
        name=table_name,
        comment=f"Raw {table_name} landed by ADF at ingest/{folder}.",
        table_properties={"quality": "bronze"},
    )
    def _table():
        return (
            spark.readStream.format("cloudFiles")
            .options(**AUTOLOADER_OPTIONS)
            .load(f"{RAW_ROOT}/{folder}/")
            .select(
                "*",
                col("_metadata.file_path").alias("_source_file"),
                col("_metadata.file_modification_time").alias("_source_modified_at"),
                current_timestamp().alias("_ingested_at"),
                expr("uuid()").alias("_batch_id"),
            )
        )

    return _table

for name, folder in SOURCE_FILES.items():
    make_bronze_table(name, folder)
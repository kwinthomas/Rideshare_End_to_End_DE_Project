from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, expr
from pyspark.sql.types import StringType

EH_NAMESPACE = spark.conf.get("eventhub_namespace")
EH_TOPIC = spark.conf.get("eventhub_topic")
EH_CONN_STR = spark.conf.get("eventhub_connection_string")

SASL_CONFIG = (
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
    'username="$ConnectionString" '
    f'password="{EH_CONN_STR}";'
)

KAFKA_OPTIONS = {
    "kafka.bootstrap.servers": f"{EH_NAMESPACE}.servicebus.windows.net:9093",
    "subscribe": EH_TOPIC,
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": SASL_CONFIG,
    "kafka.request.timeout.ms": "60000",
    "kafka.session.timeout.ms": "60000",
    "startingOffsets": "earliest",
    "failOnDataLoss": "true",
    "maxOffsetsPerTrigger": "10000",
}


@dp.table(
    name="rides_raw",
    comment="Raw ride events from Event Hub.",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "false",
    },
)
@dp.expect_or_fail("value_not_null", "ride_payload IS NOT NULL")
def rides_raw():
    return (
        spark.readStream.format("kafka")
        .options(**KAFKA_OPTIONS)
        .load()
        .select(
            col("key").cast(StringType()).alias("kafka_key"),
            col("value").cast(StringType()).alias("ride_payload"),
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_enqueued_at"),
            current_timestamp().alias("_ingested_at"),
            expr("uuid()").alias("_batch_id"),
        )
    )
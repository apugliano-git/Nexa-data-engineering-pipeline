import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col


BROKER = "redpanda:29092"
TOPIC = "reservation.events.v1"
BRONZE_PATH = "/opt/nexa/data/bronze"
CHECKPOINT_PATH = "/opt/nexa/data/checkpoint/bronze"

logger = logging.getLogger("nexa.bronze")


def to_bronze(kafka_df: DataFrame) -> DataFrame:
    return kafka_df.select(
        col("key").cast("string").alias("key"),
        col("value").cast("string").alias("value"),
        col("topic").cast("string").alias("topic"),
        col("partition").cast("int").alias("partition"),
        col("offset").cast("long").alias("offset"),
        col("timestamp").cast("timestamp").alias("timestamp"),
    )


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("nexa-bronze-ingest")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        logger.warning(
            "Starting Bronze ingestion topic=%s broker=%s bronze=%s checkpoint=%s",
            TOPIC,
            BROKER,
            BRONZE_PATH,
            CHECKPOINT_PATH,
        )

        kafka_df = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", BROKER)
            .option("subscribe", TOPIC)
            .option("startingOffsets", "latest")
            .load()
        )

        query = (
            to_bronze(kafka_df)
            .writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", CHECKPOINT_PATH)
            .start(BRONZE_PATH)
        )
        query.awaitTermination()
    except Exception:
        logger.exception("Bronze ingestion stopped with an error")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    main()

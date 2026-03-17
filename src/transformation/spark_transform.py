from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
log = logging.getLogger(__name__)

# Start Spark session
spark = SparkSession.builder \
    .appName("WistiaVideoAnalytics") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

log.info("=" * 50)
log.info("PySpark transformation started")

# Media IDs and their channels
MEDIA_IDS = {
    "gskhw4w4lm": "Facebook",
    "v08dlrgr7v": "YouTube"
}

# ── dim_media ──────────────────────────────────────
log.info("Building dim_media...")

metadata_dfs = []
for media_id, channel in MEDIA_IDS.items():
    path = f"data/raw/metadata/{media_id}/date=*/metadata.json"
    try:
        df = spark.read.json(path)
        df = df.withColumn("media_id", lit(media_id)) \
               .withColumn("channel", lit(channel))
        metadata_dfs.append(df)
    except Exception as e:
        log.error(f"Error reading metadata for {media_id}: {e}")

if metadata_dfs:
    dim_media = metadata_dfs[0]
    for df in metadata_dfs[1:]:
        dim_media = dim_media.unionByName(df, allowMissingColumns=True)

    dim_media = dim_media.select(
        col("media_id"),
        col("channel")
    ).dropDuplicates(["media_id"])

    log.info(f"dim_media rows: {dim_media.count()}")
    dim_media.show()

    # Save as parquet
    dim_media.write.mode("overwrite").parquet("data/processed/dim_media")
    log.info("dim_media saved!")

# ── dim_visitor & fact_media_engagement ───────────
log.info("Building dim_visitor and fact_media_engagement...")

events_dfs = []
for media_id, channel in MEDIA_IDS.items():
    path = f"data/raw/{media_id}/date=*/events.json"
    try:
        df = spark.read.option("multiLine", "true").json(path)
        df = df.withColumn("media_id", lit(media_id))
        events_dfs.append(df)
    except Exception as e:
        log.error(f"Error reading events for {media_id}: {e}")

if events_dfs:
    events = events_dfs[0]
    for df in events_dfs[1:]:
        events = events.unionByName(df, allowMissingColumns=True)

    # dim_visitor
    dim_visitor = events.select(
        col("visitor_key").alias("visitor_id"),
        col("ip").alias("ip_address"),
        col("country")
    ).dropDuplicates(["visitor_id"])

    log.info(f"dim_visitor rows: {dim_visitor.count()}")
    dim_visitor.show(5)
    dim_visitor.write.mode("overwrite").parquet("data/processed/dim_visitor")
    log.info("dim_visitor saved!")

    # fact_media_engagement
    fact = events.select(
        col("media_id"),
        col("visitor_key").alias("visitor_id"),
        to_date(col("received_at")).alias("date"),
        col("percent_viewed").alias("watched_percent")
    ).dropDuplicates()

    log.info(f"fact_media_engagement rows: {fact.count()}")
    fact.show(5)
    fact.write.mode("overwrite").parquet("data/processed/fact_media_engagement")
    log.info("fact_media_engagement saved!")

log.info("PySpark transformation finished!")
log.info("=" * 50)

spark.stop()
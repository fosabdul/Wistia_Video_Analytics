from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date, row_number, desc
from pyspark.sql.window import Window
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
log = logging.getLogger(__name__)

spark = SparkSession.builder.appName("WistiaVideoAnalytics").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

log.info("=" * 50)
log.info("PySpark transformation started")

MEDIA_IDS = {"gskhw4w4lm": "Facebook", "v08dlrgr7v": "YouTube"}

log.info("Building dim_media...")
metadata_dfs = []
for media_id in MEDIA_IDS:
    path = f"data/raw/metadata/{media_id}/date=*/metadata.json"
    df = spark.read.option("multiLine", "true").json(path)
    metadata_dfs.append(df)

dim_media = metadata_dfs[0]
for df in metadata_dfs[1:]:
    dim_media = dim_media.unionByName(df, allowMissingColumns=True)

dim_media = dim_media.select(
    col("media_id"), col("channel"), col("title"), col("url"),
    col("created_at"), col("hashed_id"), col("play_count"),
    col("play_rate"), col("hours_watched"), col("engagement"), col("visitors")
).dropDuplicates(["media_id"])

log.info(f"dim_media rows: {dim_media.count()}")
dim_media.show()
dim_media.write.mode("overwrite").parquet("data/processed/dim_media")
log.info("dim_media saved!")

log.info("Building dim_visitor and fact_media_engagement...")
events_dfs = []
for media_id in MEDIA_IDS:
    path = f"data/raw/{media_id}/date=*/events.json"
    df = spark.read.option("multiLine", "true").json(path)
    df = df.withColumn("media_id", lit(media_id))
    events_dfs.append(df)

events = events_dfs[0]
for df in events_dfs[1:]:
    events = events.unionByName(df, allowMissingColumns=True)

dim_visitor = events.select(
    col("visitor_key").alias("visitor_id"),
    col("ip").alias("ip_address"),
    col("country")
).dropDuplicates(["visitor_id"])

log.info(f"dim_visitor rows: {dim_visitor.count()}")
dim_visitor.show(5)
dim_visitor.write.mode("overwrite").parquet("data/processed/dim_visitor")
log.info("dim_visitor saved!")

fact_raw = events.select(
    col("media_id"),
    col("visitor_key").alias("visitor_id"),
    to_date(col("received_at")).alias("date"),
    col("percent_viewed").alias("watched_percent")
)

window = Window.partitionBy("media_id", "visitor_id", "date").orderBy(desc("watched_percent"))
fact = fact_raw.withColumn("row_num", row_number().over(window)).filter(col("row_num") == 1).drop("row_num")

log.info(f"fact_media_engagement rows: {fact.count()}")
fact.show(5)
fact.write.mode("overwrite").parquet("data/processed/fact_media_engagement")
log.info("fact_media_engagement saved!")

log.info("PySpark transformation finished!")
log.info("=" * 50)
spark.stop()

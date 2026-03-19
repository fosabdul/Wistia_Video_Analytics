
Architecture and Design Decisions
==================================

This document explains the key technical decisions made during the
design and implementation of the Wistia Video Analytics Pipeline.


1. Choice of Tools
------------------
Python was chosen for API ingestion because it has excellent support
for HTTP requests, JSON parsing, and environment variable management.
It is also the industry standard for data engineering pipelines.

PySpark was chosen for transformation because it is designed to handle
large volumes of data efficiently. With 60,000+ records being processed
daily, a distributed processing framework ensures the pipeline can scale
as data volumes grow over time.

Cron was chosen for scheduling because it is simple, reliable, and
requires no additional infrastructure. For a pipeline running on a single
machine for 7 days, cron is the right tool for the job.

GitHub Actions was chosen for CI/CD because it integrates directly with
GitHub at no extra cost and is widely used in the industry.


2. Incremental Ingestion Strategy
----------------------------------
The pipeline uses a watermark file (last_run.json) to track the timestamp
of the last successful run. On each subsequent run, only records created
after that timestamp are fetched from the API.

This decision was made because the Wistia API returns 60,000+ records
in total. Fetching all records on every daily run would be slow, wasteful,
and could trigger API rate limits. With incremental ingestion, daily runs
complete in seconds rather than minutes.

The tradeoff is that if the watermark file is deleted or corrupted, the
pipeline will fall back to a full load. This is acceptable because a full
load is always correct, just slower.


3. Raw Data Preservation
------------------------
Raw JSON responses from the API are saved to disk before any transformation
takes place. This decision follows the medallion architecture pattern where
raw data is always preserved in its original form.

The benefit is that if the transformation logic needs to change, the raw
data can be reprocessed without making additional API calls. This also
provides an audit trail of exactly what data was received from the API.


4. Separation of Concerns
--------------------------
Ingestion, transformation, and orchestration are kept in completely separate
scripts rather than one large file. This decision makes each component
easier to test, debug, and modify independently.

For example, if the API endpoint changes, only the ingestion script needs
to be updated. The transformation and orchestration scripts remain unchanged.


5. Error Handling and Retry Logic
-----------------------------------
Every API call includes retry logic with exponential backoff. If a request
fails, the pipeline waits 2 seconds before retrying, then 4 seconds, then
8 seconds. This handles temporary network issues and API rate limiting
without requiring manual intervention.

All errors are logged with timestamps so any failure can be diagnosed
quickly by reviewing the pipeline.log file.


6. Data Model Design
---------------------
The data model follows a simple star schema with one fact table and two
dimension tables. This design was chosen because it is easy to understand,
query efficiently, and extend in the future.

The fact table (fact_media_engagement) stores one row per visitor per
video per day. The dimension tables (dim_media and dim_visitor) store
descriptive attributes that change slowly over time.


7. Assumptions Made
--------------------
The channel assignment (Facebook or YouTube) was manually mapped to
each media ID since the API does not return this information directly.

The visitor_key field in the API response was used as the unique
visitor identifier since no explicit visitor_id field was available.

The received_at field was used as the event timestamp for date
partitioning in the fact table.


8. Known Limitations
---------------------
The pipeline currently runs on a local machine using cron. In a
production environment this would be moved to a cloud scheduler
such as AWS EventBridge or Apache Airflow.

The processed data is stored as local Parquet files synced to S3.
In production these would be loaded directly into a cloud data
warehouse such as Amazon Redshift or Azure Synapse Analytics.

The pipeline does not currently send alerts on failure. In production
this would be added using email notifications or Slack webhooks.


9. Idempotency and Exactly-Once Behavior
------------------------------------------
The pipeline is designed to be safe to re-run multiple times without
producing duplicate data. This is achieved through three mechanisms:

Watermark checkpointing
  The last_run.json file acts as a checkpoint store. It records the
  exact timestamp of the last successful run. If the pipeline is
  re-run on the same day, the incremental logic will only fetch
  records created after that timestamp, preventing duplicate ingestion.

Deterministic deduplication
  The fact table uses a window function that partitions by the
  composite key (media_id + visitor_id + date) and keeps only the
  row with the highest watched_percent. This means re-running the
  transformation on the same raw data will always produce exactly
  the same output.

Overwrite mode
  All Parquet files are written using mode overwrite. This means
  re-running the transformation completely replaces the previous
  output rather than appending to it, guaranteeing idempotency.

If a backfill is needed, the last_run.json file can be deleted to
trigger a full reload. The deduplication logic ensures the output
will still be correct even if the same records are ingested twice.


10. Data Contracts — Grain and Primary Key Rules
-------------------------------------------------
Each table in the data model has a clearly defined grain and primary
key to ensure data integrity and prevent ambiguous joins.

dim_media
  Grain: One row per video asset
  Primary key: media_id
  Uniqueness rule: media_id must be unique — enforced via dropDuplicates
  Example: gskhw4w4lm represents the Facebook video asset

dim_visitor
  Grain: One row per unique visitor across all videos
  Primary key: visitor_id
  Uniqueness rule: visitor_id must be unique — enforced via dropDuplicates
  Example: one visitor may have watched both videos but appears once here

fact_media_engagement
  Grain: One row per visitor per video per day
  Composite primary key: media_id + visitor_id + date
  Uniqueness rule: composite key must be unique — enforced via window
  function that keeps the highest watched_percent per key
  Example: if a visitor watched the same video twice on the same day,
  only the session with the highest watch percentage is kept
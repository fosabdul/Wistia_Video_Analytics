import logging
import pandas as pd
from datetime import datetime, timezone, timedelta

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

def run_quality_checks():
    log.info("=" * 50)
    log.info("Running data quality checks...")
    passed = 0
    failed = 0

    # Load processed tables
    dim_media = pd.read_parquet("data/processed/dim_media")
    dim_visitor = pd.read_parquet("data/processed/dim_visitor")
    fact = pd.read_parquet("data/processed/fact_media_engagement")

    # ── NULL CHECKS ──────────────────────────────
    log.info("Running null checks...")

    # dim_media null check
    null_media = dim_media["media_id"].isnull().sum()
    if null_media == 0:
        log.info("✅ dim_media.media_id — no nulls")
        passed += 1
    else:
        log.error(f"❌ dim_media.media_id — {null_media} null values found!")
        failed += 1

    # dim_visitor null check
    null_visitor = dim_visitor["visitor_id"].isnull().sum()
    if null_visitor == 0:
        log.info("✅ dim_visitor.visitor_id — no nulls")
        passed += 1
    else:
        log.error(f"❌ dim_visitor.visitor_id — {null_visitor} null values found!")
        failed += 1

    # fact null checks
    for col in ["media_id", "visitor_id", "date"]:
        null_count = fact[col].isnull().sum()
        if null_count == 0:
            log.info(f"✅ fact.{col} — no nulls")
            passed += 1
        else:
            log.error(f"❌ fact.{col} — {null_count} null values found!")
            failed += 1

    # ── UNIQUENESS CHECKS ─────────────────────────
    log.info("Running uniqueness checks...")

    # dim_media uniqueness
    dupes_media = dim_media["media_id"].duplicated().sum()
    if dupes_media == 0:
        log.info("✅ dim_media.media_id — no duplicates")
        passed += 1
    else:
        log.error(f"❌ dim_media.media_id — {dupes_media} duplicates found!")
        failed += 1

    # dim_visitor uniqueness
    dupes_visitor = dim_visitor["visitor_id"].duplicated().sum()
    if dupes_visitor == 0:
        log.info("✅ dim_visitor.visitor_id — no duplicates")
        passed += 1
    else:
        log.error(f"❌ dim_visitor.visitor_id — {dupes_visitor} duplicates found!")
        failed += 1

    # fact composite key uniqueness
    dupes_fact = fact.duplicated(
        subset=["media_id", "visitor_id", "date"]
    ).sum()
    if dupes_fact == 0:
        log.info("✅ fact composite key (media_id+visitor_id+date) — no duplicates")
        passed += 1
    else:
        log.error(f"❌ fact composite key — {dupes_fact} duplicates found!")
        failed += 1

    # ── SCHEMA CHECKS ────────────────────────────
    log.info("Running schema checks...")

    expected_media_cols = ["media_id", "channel", "title", "play_count", "play_rate"]
    expected_visitor_cols = ["visitor_id", "ip_address", "country"]
    expected_fact_cols = ["media_id", "visitor_id", "date", "watched_percent"]

    for col in expected_media_cols:
        if col in dim_media.columns:
            log.info(f"✅ dim_media.{col} — column exists")
            passed += 1
        else:
            log.error(f"❌ dim_media.{col} — column missing!")
            failed += 1

    for col in expected_visitor_cols:
        if col in dim_visitor.columns:
            log.info(f"✅ dim_visitor.{col} — column exists")
            passed += 1
        else:
            log.error(f"❌ dim_visitor.{col} — column missing!")
            failed += 1

    for col in expected_fact_cols:
        if col in fact.columns:
            log.info(f"✅ fact.{col} — column exists")
            passed += 1
        else:
            log.error(f"❌ fact.{col} — column missing!")
            failed += 1

    # ── ROW COUNT CHECKS ─────────────────────────
    log.info("Running row count checks...")

    if len(dim_media) >= 2:
        log.info(f"✅ dim_media — {len(dim_media)} rows (expected 2)")
        passed += 1
    else:
        log.error(f"❌ dim_media — only {len(dim_media)} rows!")
        failed += 1

    if len(dim_visitor) > 0:
        log.info(f"✅ dim_visitor — {len(dim_visitor):,} rows")
        passed += 1
    else:
        log.error("❌ dim_visitor — empty table!")
        failed += 1

    if len(fact) > 0:
        log.info(f"✅ fact_media_engagement — {len(fact):,} rows")
        passed += 1
    else:
        log.error("❌ fact_media_engagement — empty table!")
        failed += 1

    # ── FRESHNESS CHECK ──────────────────────────
    log.info("Running freshness check...")

    fact["date"] = pd.to_datetime(fact["date"])
    latest_date = fact["date"].max()
    days_old = (datetime.now(timezone.utc).date() - latest_date.date()).days

    if days_old <= 30:
        log.info(f"✅ Data freshness — latest record is {days_old} day(s) old")
        passed += 1
    else:
        log.warning(f"⚠️ Data freshness — latest record is {days_old} days old!")
        failed += 1

    # ── SUMMARY ──────────────────────────────────
    log.info("=" * 50)
    log.info(f"Quality check results: {passed} passed · {failed} failed")

    if failed > 0:
        log.error(f"❌ Quality checks FAILED — {failed} issues found!")
        return False
    else:
        log.info("✅ All quality checks passed!")
        return True

if __name__ == "__main__":
    success = run_quality_checks()
    if not success:
        exit(1)
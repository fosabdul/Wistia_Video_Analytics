import subprocess
import logging
from datetime import datetime, timezone

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

def run_script(script_path):
    """
    Runs a Python script and logs the result.
    Returns True if successful, False if failed.
    """
    log.info(f"Running {script_path}...")
    result = subprocess.run(
        ["python3", script_path],
        capture_output=True,
        text=True
    )
    if result.returncode == 0:
        log.info(f"✅ {script_path} completed successfully!")
        return True
    else:
        log.error(f"❌ {script_path} failed!")
        log.error(result.stderr)
        return False

# Run the full pipeline
log.info("=" * 50)
log.info(f"🚀 Pipeline started at {datetime.now(timezone.utc)}")
log.info("=" * 50)

# Step 1 — Extract media metadata
if not run_script("src/ingestion/media_metadata.py"):
    log.error("Pipeline stopped at media metadata extraction!")
    exit(1)

# Step 2 — Extract visitor events
if not run_script("src/ingestion/wistia_ingest.py"):
    log.error("Pipeline stopped at visitor events extraction!")
    exit(1)

# Step 3 — Transform raw data with PySpark
if not run_script("src/transformation/spark_transform.py"):
    log.error("Pipeline stopped at transformation!")
    exit(1)

log.info("=" * 50)
log.info(f"✅ Pipeline finished successfully at {datetime.now(timezone.utc)}")
log.info("=" * 50)
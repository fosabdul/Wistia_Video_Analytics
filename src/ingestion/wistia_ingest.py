import requests
import json
import os
import logging
from dotenv import load_dotenv
from datetime import datetime, timezone
from time import sleep

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

# Load API token from .env file
load_dotenv()
API_TOKEN = os.getenv("WISTIA_API_TOKEN")

# Media IDs to pull
MEDIA_IDS = ["gskhw4w4lm", "v08dlrgr7v"]

# File to track last run time
WATERMARK_FILE = "last_run.json"

def get_last_run():
    if os.path.exists(WATERMARK_FILE):
        with open(WATERMARK_FILE, "r") as f:
            return json.load(f).get("last_run")
    return None

def save_last_run():
    with open(WATERMARK_FILE, "w") as f:
        json.dump({"last_run": datetime.now(timezone.utc).isoformat()}, f)
    log.info("Watermark saved successfully!")

def get_visitor_events(media_id, last_run=None):
    all_events = []
    page = 1
    per_page = 100
    max_retries = 3

    while True:
        url = "https://api.wistia.com/v1/stats/events.json"
        auth = ("api", API_TOKEN)
        params = {"media_id": media_id, "page": page, "per_page": per_page}

        if last_run:
            params["start_date"] = last_run

        for attempt in range(max_retries):
            try:
                response = requests.get(url, auth=auth, params=params, timeout=30)

                if response.status_code == 200:
                    data = response.json()
                    if not data:
                        return all_events
                    all_events.extend(data)
                    log.info(f"Page {page} fetched — {len(data)} records")
                    page += 1
                    break

                elif response.status_code == 429:
                    log.warning("Rate limited! Waiting 60 seconds...")
                    sleep(60)

                else:
                    log.error(f"Error {response.status_code} on attempt {attempt+1}")
                    sleep(2 ** attempt)

            except Exception as e:
                log.error(f"Exception on attempt {attempt+1}: {e}")
                sleep(2 ** attempt)

    return all_events

# Run incremental ingestion
log.info("=" * 50)
log.info("Pipeline started")
last_run = get_last_run()

if last_run:
    log.info(f"Incremental run — fetching records since {last_run}")
else:
    log.info("First run — fetching all records")

for media_id in MEDIA_IDS:
    log.info(f"Fetching events for {media_id}")
    events = get_visitor_events(media_id, last_run)
    log.info(f"Total records fetched for {media_id}: {len(events)}")

    # Save raw data locally
    if events:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        output_dir = f"data/raw/{media_id}/date={date_str}"
        os.makedirs(output_dir, exist_ok=True)
        output_file = f"{output_dir}/events.json"

        with open(output_file, "w") as f:
            json.dump(events, f, indent=2)

        log.info(f"Raw data saved to {output_file}")
    else:
        log.info(f"No new records to save for {media_id}")

save_last_run()
log.info("Pipeline finished successfully!")
log.info("=" * 50)
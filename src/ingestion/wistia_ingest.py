import requests
import json
import os
from dotenv import load_dotenv
from datetime import datetime, timezone

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
    print(f"✅ Watermark saved!")

def get_visitor_events(media_id, last_run=None):
    all_events = []
    page = 1
    per_page = 100

    while True:
        url = f"https://api.wistia.com/v1/stats/events.json"
        auth = ("api", API_TOKEN)
        params = {"media_id": media_id, "page": page, "per_page": per_page}

        if last_run:
            params["start_date"] = last_run

        response = requests.get(url, auth=auth, params=params)

        if response.status_code == 200:
            data = response.json()
            if not data:
                break
            all_events.extend(data)
            print(f"✅ Page {page} fetched — {len(data)} records")
            page += 1
        else:
            print(f"❌ Error {response.status_code}")
            break

    return all_events

# Run incremental ingestion
last_run = get_last_run()

if last_run:
    print(f"📅 Incremental run — fetching records since {last_run}")
else:
    print(f"🚀 First run — fetching all records")

for media_id in MEDIA_IDS:
    print(f"\n--- Fetching events for {media_id} ---")
    events = get_visitor_events(media_id, last_run)
    print(f"Total records fetched: {len(events)}")

save_last_run()
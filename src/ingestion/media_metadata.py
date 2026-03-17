import requests
import json
import os
import logging
from dotenv import load_dotenv
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

# Load API token from .env file
load_dotenv()
API_TOKEN = os.getenv("WISTIA_API_TOKEN")

# Media IDs and their channels
MEDIA_IDS = {
    "gskhw4w4lm": "Facebook",
    "v08dlrgr7v": "YouTube"
}

def get_media_metadata(media_id):
    """
    Fetches media level stats like title, 
    play count, play rate and watch time
    """
    url = f"https://api.wistia.com/v1/stats/medias/{media_id}.json"
    auth = ("api", API_TOKEN)

    try:
        response = requests.get(url, auth=auth, timeout=30)

        if response.status_code == 200:
            data = response.json()
            log.info(f"✅ Metadata fetched for {media_id}")
            return data
        else:
            log.error(f"❌ Error {response.status_code} for {media_id}")
            return None

    except Exception as e:
        log.error(f"❌ Exception for {media_id}: {e}")
        return None

def save_metadata(media_id, data):
    """
    Saves raw metadata JSON to data/raw folder
    partitioned by date
    """
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    output_dir = f"data/raw/metadata/{media_id}/date={date_str}"
    os.makedirs(output_dir, exist_ok=True)
    output_file = f"{output_dir}/metadata.json"

    with open(output_file, "w") as f:
        json.dump(data, f, indent=2)

    log.info(f"💾 Metadata saved to {output_file}")

# Run metadata extraction
log.info("=" * 50)
log.info("Media metadata extraction started")

for media_id, channel in MEDIA_IDS.items():
    log.info(f"Fetching metadata for {media_id} ({channel})")
    metadata = get_media_metadata(media_id)

    if metadata:
        # Add channel info manually
        metadata["channel"] = channel
        save_metadata(media_id, metadata)
        log.info(f"📊 Stats — Plays: {metadata.get('play_count')} | Play rate: {metadata.get('play_rate')}")

log.info("Media metadata extraction finished!")
log.info("=" * 50)
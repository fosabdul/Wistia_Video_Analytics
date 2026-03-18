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
    """Fetches media level stats like play count and play rate"""
    url = f"https://api.wistia.com/v1/stats/medias/{media_id}.json"
    auth = ("api", API_TOKEN)
    try:
        response = requests.get(url, auth=auth, timeout=30)
        if response.status_code == 200:
            log.info(f"✅ Stats fetched for {media_id}")
            return response.json()
        else:
            log.error(f"❌ Error {response.status_code} for stats {media_id}")
            return {}
    except Exception as e:
        log.error(f"❌ Exception: {e}")
        return {}

def get_media_details(media_id):
    """Fetches full media details including title, url, created_at"""
    url = f"https://api.wistia.com/v1/medias/{media_id}.json"
    auth = ("api", API_TOKEN)
    try:
        response = requests.get(url, auth=auth, timeout=30)
        if response.status_code == 200:
            log.info(f"✅ Details fetched for {media_id}")
            return response.json()
        else:
            log.error(f"❌ Error {response.status_code} for details {media_id}")
            return {}
    except Exception as e:
        log.error(f"❌ Exception: {e}")
        return {}

def save_metadata(media_id, data):
    """Saves combined metadata JSON to data/raw folder"""
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

    # Get stats
    stats = get_media_metadata(media_id)

    # Get media details (title, url, created_at)
    details = get_media_details(media_id)

    if stats:
        combined = {
            "media_id": media_id,
            "channel": channel,
            "title": details.get("name", ""),
            "url": details.get("url", ""),
            "created_at": details.get("created", ""),
            "hashed_id": details.get("hashed_id", ""),
            "play_count": stats.get("play_count", 0),
            "play_rate": stats.get("play_rate", 0),
            "hours_watched": stats.get("hours_watched", 0),
            "engagement": stats.get("engagement", 0),
            "visitors": stats.get("visitors", 0)
        }
        save_metadata(media_id, combined)
        log.info(f"Title: {combined['title']} | Plays: {combined['play_count']}")

log.info("Media metadata extraction finished!")
log.info("=" * 50)
import requests
import json
import os
from dotenv import load_dotenv

# Load API token from .env file
load_dotenv()
API_TOKEN = os.getenv("WISTIA_API_TOKEN")

# Media IDs to pull
MEDIA_IDS = ["gskhw4w4lm", "v08dlrgr7v"]

def get_media_stats(media_id):
    url = f"https://api.wistia.com/v1/stats/medias/{media_id}.json"
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        print(f"✅ Success for media: {media_id}")
        return response.json()
    else:
        print(f"❌ Error {response.status_code} for media: {media_id}")
        return None

# Test it
for media_id in MEDIA_IDS:
    data = get_media_stats(media_id)
    print(json.dumps(data, indent=2))
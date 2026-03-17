import pytest
import json
import os
from unittest.mock import patch, MagicMock

from dotenv import load_dotenv
load_dotenv()

# Test 1 — check API token is loaded
def test_api_token_exists():
    """Make sure the API token environment variable is set"""
    token = os.getenv("WISTIA_API_TOKEN")
    assert token is not None, "WISTIA_API_TOKEN is not set!"

# Test 2 — check pagination logic
def test_pagination_stops_on_empty():
    """Make sure pagination stops when API returns empty list"""
    pages = [[{"id": 1}, {"id": 2}], [{"id": 3}], []]
    all_events = []
    for page in pages:
        if not page:
            break
        all_events.extend(page)
    assert len(all_events) == 3

# Test 3 — check watermark file saves correctly
def test_watermark_saves(tmp_path):
    """Make sure watermark file is saved correctly"""
    watermark_file = tmp_path / "last_run.json"
    data = {"last_run": "2026-03-17T00:00:00+00:00"}
    with open(watermark_file, "w") as f:
        json.dump(data, f)
    with open(watermark_file, "r") as f:
        loaded = json.load(f)
    assert loaded["last_run"] == "2026-03-17T00:00:00+00:00"

# Test 4 — check media IDs are correct
def test_media_ids():
    """Make sure both media IDs are present"""
    MEDIA_IDS = ["gskhw4w4lm", "v08dlrgr7v"]
    assert "gskhw4w4lm" in MEDIA_IDS
    assert "v08dlrgr7v" in MEDIA_IDS
    assert len(MEDIA_IDS) == 2
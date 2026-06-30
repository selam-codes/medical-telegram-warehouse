import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.scraper import message_to_record


def test_message_to_record_basic_fields():
    message = SimpleNamespace(
        id=42,
        date=datetime(2026, 6, 1, tzinfo=timezone.utc),
        message="Paracetamol 500mg available",
        media=None,
        views=10,
        forwards=2,
        to_dict=lambda: {"id": 42, "date": "2026-06-01T00:00:00+00:00"},
    )

    record = message_to_record(message, "tikvahpharma", None)

    assert record["message_id"] == 42
    assert record["channel_name"] == "tikvahpharma"
    assert record["message_text"] == "Paracetamol 500mg available"
    assert record["has_media"] is False
    assert record["views"] == 10
    assert record["forwards"] == 2

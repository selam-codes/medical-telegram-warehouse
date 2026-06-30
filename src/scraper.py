"""
Telegram scraper for the medical/pharma data warehouse project.

Extracts messages (+ images) from a configured list of public Telegram
channels and writes them into a partitioned raw data lake:

    data/raw/telegram_messages/YYYY-MM-DD/{channel_name}.json
    data/raw/images/{channel_name}/{message_id}.jpg

Run:
    python src/scraper.py
"""

import asyncio
import json
import logging
import os
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import FloodWaitError, ChannelPrivateError, UsernameNotOccupiedError
from telethon.tl.types import MessageMediaPhoto

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "raw"
MESSAGES_DIR = DATA_DIR / "telegram_messages"
IMAGES_DIR = DATA_DIR / "images"
LOGS_DIR = BASE_DIR / "logs"

LOGS_DIR.mkdir(parents=True, exist_ok=True)

API_ID = os.getenv("TELEGRAM_API_ID")
API_HASH = os.getenv("TELEGRAM_API_HASH")
PHONE = os.getenv("TELEGRAM_PHONE")
SESSION_NAME = os.getenv("TELEGRAM_SESSION_NAME", "medical_warehouse")
CHANNELS = [c.strip() for c in os.getenv("TELEGRAM_CHANNELS", "").split(",") if c.strip()]

# how many days back to pull on each run; messages already saved are skipped via dedup
MAX_MESSAGES_PER_CHANNEL = int(os.getenv("MAX_MESSAGES_PER_CHANNEL", "500"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / f"scraper_{datetime.now(timezone.utc):%Y-%m-%d}.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("scraper")


def message_to_record(message, channel_name: str, image_path: str | None) -> dict:
    """Preserve Telethon's native message structure, plus our own derived fields."""
    raw = message.to_dict()
    return {
        "message_id": message.id,
        "channel_name": channel_name,
        "message_date": message.date.isoformat() if message.date else None,
        "message_text": message.message or "",
        "has_media": message.media is not None,
        "image_path": image_path,
        "views": getattr(message, "views", None) or 0,
        "forwards": getattr(message, "forwards", None) or 0,
        "raw": json.loads(json.dumps(raw, default=str)),
    }


async def download_image(client: TelegramClient, message, channel_name: str) -> str | None:
    if not isinstance(message.media, MessageMediaPhoto):
        return None

    channel_image_dir = IMAGES_DIR / channel_name
    channel_image_dir.mkdir(parents=True, exist_ok=True)
    target_path = channel_image_dir / f"{message.id}.jpg"

    if target_path.exists():
        return str(target_path.relative_to(BASE_DIR))

    try:
        await client.download_media(message, file=str(target_path))
        return str(target_path.relative_to(BASE_DIR))
    except Exception as exc:  # noqa: BLE001 — log and continue scraping
        logger.error("Failed to download image for %s/%s: %s", channel_name, message.id, exc)
        return None


async def scrape_channel(client: TelegramClient, channel_name: str) -> int:
    """Scrape a single channel and write one JSON file per day visited."""
    logger.info("Starting scrape for channel: %s", channel_name)
    records_by_date: dict[str, list[dict]] = defaultdict(list)
    count = 0

    try:
        entity = await client.get_entity(channel_name)
    except (ChannelPrivateError, UsernameNotOccupiedError) as exc:
        logger.error("Cannot access channel %s: %s", channel_name, exc)
        return 0

    try:
        async for message in client.iter_messages(entity, limit=MAX_MESSAGES_PER_CHANNEL):
            if message.message is None and message.media is None:
                continue  # service messages, skip

            image_path = await download_image(client, message, channel_name)
            record = message_to_record(message, channel_name, image_path)

            date_str = message.date.strftime("%Y-%m-%d") if message.date else "unknown-date"
            records_by_date[date_str].append(record)
            count += 1
    except FloodWaitError as exc:
        logger.warning("Rate limited on %s, must wait %s seconds", channel_name, exc.seconds)
        await asyncio.sleep(exc.seconds)
    except Exception as exc:  # noqa: BLE001 — keep scraping other channels
        logger.error("Unexpected error scraping %s: %s", channel_name, exc)

    for date_str, records in records_by_date.items():
        partition_dir = MESSAGES_DIR / date_str
        partition_dir.mkdir(parents=True, exist_ok=True)
        out_path = partition_dir / f"{channel_name}.json"

        existing = []
        if out_path.exists():
            with open(out_path, "r", encoding="utf-8") as f:
                existing = json.load(f)

        existing_ids = {r["message_id"] for r in existing}
        merged = existing + [r for r in records if r["message_id"] not in existing_ids]

        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(merged, f, ensure_ascii=False, indent=2, default=str)

        logger.info("Wrote %d messages to %s", len(merged), out_path)

    logger.info("Finished scrape for %s: %d messages processed", channel_name, count)
    return count


async def main():
    if not API_ID or not API_HASH:
        logger.error(
            "TELEGRAM_API_ID / TELEGRAM_API_HASH not set. Copy .env.example to .env and fill "
            "in credentials from https://my.telegram.org before running the scraper."
        )
        return

    if not CHANNELS:
        logger.error("No channels configured in TELEGRAM_CHANNELS.")
        return

    client = TelegramClient(SESSION_NAME, int(API_ID), API_HASH)
    await client.start(phone=PHONE)

    total = 0
    for channel_name in CHANNELS:
        total += await scrape_channel(client, channel_name)

    await client.disconnect()
    logger.info("Scraping run complete. Total messages processed: %d", total)


if __name__ == "__main__":
    asyncio.run(main())

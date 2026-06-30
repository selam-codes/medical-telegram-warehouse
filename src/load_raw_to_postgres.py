"""
Loads partitioned JSON files from the raw data lake (data/raw/telegram_messages/)
into a `raw.telegram_messages` table in PostgreSQL.

Run:
    python src/load_raw_to_postgres.py
"""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
MESSAGES_DIR = BASE_DIR / "data" / "raw" / "telegram_messages"
LOGS_DIR = BASE_DIR / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / f"load_raw_{datetime.now(timezone.utc):%Y-%m-%d}.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("load_raw_to_postgres")

DDL = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.telegram_messages (
    id SERIAL PRIMARY KEY,
    message_id BIGINT NOT NULL,
    channel_name TEXT NOT NULL,
    message_date TIMESTAMPTZ,
    message_text TEXT,
    has_media BOOLEAN,
    image_path TEXT,
    views INTEGER,
    forwards INTEGER,
    raw_payload JSONB,
    loaded_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (channel_name, message_id)
);
"""

UPSERT = """
INSERT INTO raw.telegram_messages
    (message_id, channel_name, message_date, message_text, has_media, image_path, views, forwards, raw_payload)
VALUES
    (%(message_id)s, %(channel_name)s, %(message_date)s, %(message_text)s, %(has_media)s,
     %(image_path)s, %(views)s, %(forwards)s, %(raw_payload)s)
ON CONFLICT (channel_name, message_id) DO UPDATE SET
    message_date = EXCLUDED.message_date,
    message_text = EXCLUDED.message_text,
    has_media = EXCLUDED.has_media,
    image_path = EXCLUDED.image_path,
    views = EXCLUDED.views,
    forwards = EXCLUDED.forwards,
    raw_payload = EXCLUDED.raw_payload,
    loaded_at = now();
"""


def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "medical_warehouse"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
    )


def iter_message_files():
    if not MESSAGES_DIR.exists():
        logger.warning("No data lake found at %s", MESSAGES_DIR)
        return
    for date_dir in sorted(MESSAGES_DIR.iterdir()):
        if not date_dir.is_dir():
            continue
        for json_file in sorted(date_dir.glob("*.json")):
            yield json_file


def load_file(cursor, json_file: Path) -> int:
    with open(json_file, "r", encoding="utf-8") as f:
        records = json.load(f)

    rows = [
        {
            "message_id": r["message_id"],
            "channel_name": r["channel_name"],
            "message_date": r["message_date"],
            "message_text": r["message_text"],
            "has_media": r["has_media"],
            "image_path": r["image_path"],
            "views": r["views"],
            "forwards": r["forwards"],
            "raw_payload": json.dumps(r.get("raw", {})),
        }
        for r in records
    ]

    if rows:
        psycopg2.extras.execute_batch(cursor, UPSERT, rows)
    return len(rows)


def main():
    conn = get_connection()
    conn.autocommit = False
    total = 0
    try:
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()

        with conn.cursor() as cur:
            for json_file in iter_message_files():
                n = load_file(cur, json_file)
                total += n
                logger.info("Loaded %d rows from %s", n, json_file)
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("Failed to load raw data into Postgres")
        raise
    finally:
        conn.close()

    logger.info("Load complete. Total rows upserted: %d", total)


if __name__ == "__main__":
    main()

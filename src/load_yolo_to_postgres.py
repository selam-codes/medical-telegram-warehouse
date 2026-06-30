"""
Loads YOLO detection results (data/processed/yolo_detections.csv)
into raw.yolo_detections in PostgreSQL.

Run:
    python src/load_yolo_to_postgres.py
"""

import csv
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = BASE_DIR / "data" / "processed" / "yolo_detections.csv"
LOGS_DIR = BASE_DIR / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / f"load_yolo_{datetime.now(timezone.utc):%Y-%m-%d}.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("load_yolo_to_postgres")

DDL = """
CREATE TABLE IF NOT EXISTS raw.yolo_detections (
    id SERIAL PRIMARY KEY,
    channel_name TEXT NOT NULL,
    message_id BIGINT NOT NULL,
    image_path TEXT,
    detected_class TEXT,
    confidence_score NUMERIC(6,4),
    image_category TEXT,
    loaded_at TIMESTAMPTZ DEFAULT now()
);
"""


def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "/tmp"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "medical_warehouse"),
        user=os.getenv("POSTGRES_USER", "selam"),
        password=os.getenv("POSTGRES_PASSWORD") or None,
    )


def main():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(DDL)
            cur.execute("TRUNCATE raw.yolo_detections;")
        conn.commit()

        rows = []
        with open(CSV_PATH, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                rows.append({
                    "channel_name": row["channel_name"],
                    "message_id": int(row["message_id"]) if row["message_id"] else None,
                    "image_path": row["image_path"] or None,
                    "detected_class": row["detected_class"] or None,
                    "confidence_score": float(row["confidence_score"]) if row["confidence_score"] else None,
                    "image_category": row["image_category"] or None,
                })

        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(
                cur,
                """INSERT INTO raw.yolo_detections
                   (channel_name, message_id, image_path, detected_class, confidence_score, image_category)
                   VALUES (%(channel_name)s, %(message_id)s, %(image_path)s, %(detected_class)s, %(confidence_score)s, %(image_category)s)""",
                rows,
            )
        conn.commit()
        logger.info("Loaded %d detection rows into raw.yolo_detections", len(rows))
    finally:
        conn.close()


if __name__ == "__main__":
    main()

"""
Runs YOLOv8 object detection over every image downloaded in Task 1, then
derives a simple per-image content category from the detected classes.

Classification scheme (based on COCO classes YOLOv8n knows about):
    promotional     -> "person" AND a product-like object (bottle/cup/etc.) detected
    product_display -> a product-like object detected, no person
    lifestyle        -> a person detected, no product-like object
    other            -> neither detected

Run:
    python src/yolo_detect.py
"""

import csv
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from ultralytics import YOLO

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
IMAGES_DIR = BASE_DIR / "data" / "raw" / "images"
OUTPUT_DIR = BASE_DIR / "data" / "processed"
OUTPUT_CSV = OUTPUT_DIR / "yolo_detections.csv"
LOGS_DIR = BASE_DIR / "logs"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / f"yolo_detect_{datetime.now(timezone.utc):%Y-%m-%d}.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("yolo_detect")

MODEL_NAME = os.getenv("YOLO_MODEL", "yolov8n.pt")
CONFIDENCE_THRESHOLD = float(os.getenv("YOLO_CONFIDENCE", "0.25"))

# COCO classes that plausibly stand in for "a product being shown" in this
# domain (no pretrained model knows "blister pack of amoxicillin" — these are
# the closest proxies: bottles, cups, containers held/displayed by a person).
PRODUCT_LIKE_CLASSES = {
    "bottle", "cup", "bowl", "wine glass", "vase", "cell phone", "book", "box",
}


def classify_image(detected_classes: set[str]) -> str:
    has_person = "person" in detected_classes
    has_product = bool(detected_classes & PRODUCT_LIKE_CLASSES)

    if has_person and has_product:
        return "promotional"
    if has_product:
        return "product_display"
    if has_person:
        return "lifestyle"
    return "other"


def iter_images():
    if not IMAGES_DIR.exists():
        logger.warning("No images directory found at %s", IMAGES_DIR)
        return
    for channel_dir in sorted(IMAGES_DIR.iterdir()):
        if not channel_dir.is_dir():
            continue
        for image_path in sorted(channel_dir.glob("*.jpg")):
            yield channel_dir.name, image_path


def main():
    model = YOLO(MODEL_NAME)

    rows = []
    image_count = 0

    for channel_name, image_path in iter_images():
        message_id = image_path.stem
        image_count += 1

        results = model.predict(source=str(image_path), conf=CONFIDENCE_THRESHOLD, verbose=False)
        result = results[0]

        detected_classes = set()
        for box in result.boxes:
            cls_name = result.names[int(box.cls[0])]
            confidence = float(box.conf[0])
            detected_classes.add(cls_name)
            rows.append({
                "channel_name": channel_name,
                "message_id": message_id,
                "image_path": str(image_path.relative_to(BASE_DIR)),
                "detected_class": cls_name,
                "confidence_score": round(confidence, 4),
                "image_category": None,  # filled in below once we know all classes per image
            })

        category = classify_image(detected_classes)

        if not result.boxes or len(result.boxes) == 0:
            # still record the image with no detections so it shows up as "other"
            rows.append({
                "channel_name": channel_name,
                "message_id": message_id,
                "image_path": str(image_path.relative_to(BASE_DIR)),
                "detected_class": None,
                "confidence_score": None,
                "image_category": category,
            })
        else:
            for row in rows:
                if row["message_id"] == message_id and row["channel_name"] == channel_name:
                    row["image_category"] = category

        if image_count % 50 == 0:
            logger.info("Processed %d images...", image_count)

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["channel_name", "message_id", "image_path", "detected_class", "confidence_score", "image_category"],
        )
        writer.writeheader()
        writer.writerows(rows)

    logger.info("Done. Processed %d images, wrote %d detection rows to %s", image_count, len(rows), OUTPUT_CSV)


if __name__ == "__main__":
    main()

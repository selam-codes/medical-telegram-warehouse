"""
Dagster pipeline for the Medical Telegram Warehouse.

Defines 4 ops (scrape → load → dbt → yolo) wired into a single job,
plus a daily schedule.

Launch:
    dagster dev -f pipeline.py
"""

import subprocess
import sys
from pathlib import Path

from dagster import (
    op,
    job,
    schedule,
    ScheduleDefinition,
    Definitions,
    OpExecutionContext,
    In,
    Nothing,
    get_dagster_logger,
)

BASE_DIR = Path(__file__).resolve().parent
PYTHON = str(Path(sys.executable))
DBT_BIN = str(BASE_DIR / ".venv" / "bin" / "dbt")
DBT_DIR = str(BASE_DIR / "medical_warehouse")


def _run(context: OpExecutionContext, cmd: list[str], cwd: str | None = None):
    context.log.info("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=cwd or str(BASE_DIR))
    if result.stdout:
        context.log.info(result.stdout)
    if result.stderr:
        context.log.warning(result.stderr)
    if result.returncode != 0:
        raise Exception(f"Command failed (exit {result.returncode}): {' '.join(cmd)}")


@op
def scrape_telegram_data(context: OpExecutionContext):
    """Extract messages and images from Telegram channels into the raw data lake."""
    _run(context, [PYTHON, "src/scraper.py"])


@op(ins={"after_scrape": In(Nothing)})
def load_raw_to_postgres(context: OpExecutionContext):
    """Load raw JSON data lake files into raw.telegram_messages in PostgreSQL."""
    _run(context, [PYTHON, "src/load_raw_to_postgres.py"])


@op(ins={"after_load": In(Nothing)})
def run_dbt_transformations(context: OpExecutionContext):
    """Run dbt models and tests to build the star schema."""
    env_override = {"DBT_PROFILES_DIR": DBT_DIR}
    import os
    merged_env = {**os.environ, **env_override}
    context.log.info("Running dbt run...")
    result = subprocess.run(
        [DBT_BIN, "run"],
        capture_output=True, text=True, cwd=DBT_DIR, env=merged_env,
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"dbt run failed: {result.stderr}")

    context.log.info("Running dbt test...")
    result = subprocess.run(
        [DBT_BIN, "test"],
        capture_output=True, text=True, cwd=DBT_DIR, env=merged_env,
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"dbt test failed: {result.stderr}")


@op(ins={"after_dbt": In(Nothing)})
def run_yolo_enrichment(context: OpExecutionContext):
    """Run YOLOv8 object detection on downloaded images and load results to Postgres."""
    _run(context, [PYTHON, "src/yolo_detect.py"])
    _run(context, [PYTHON, "src/load_yolo_to_postgres.py"])

    # Re-run dbt to build fct_image_detections from the freshly loaded yolo data
    import os
    env_override = {"DBT_PROFILES_DIR": DBT_DIR}
    merged_env = {**os.environ, **env_override}
    result = subprocess.run(
        [DBT_BIN, "run", "--select", "fct_image_detections"],
        capture_output=True, text=True, cwd=DBT_DIR, env=merged_env,
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"dbt run fct_image_detections failed: {result.stderr}")


@job(description="End-to-end Medical Telegram Warehouse pipeline: scrape → load → transform → enrich")
def medical_warehouse_pipeline():
    raw = scrape_telegram_data()
    loaded = load_raw_to_postgres(after_scrape=raw)
    transformed = run_dbt_transformations(after_load=loaded)
    run_yolo_enrichment(after_dbt=transformed)


daily_schedule = ScheduleDefinition(
    job=medical_warehouse_pipeline,
    cron_schedule="0 3 * * *",  # 03:00 AM UTC daily
    name="daily_medical_warehouse",
)

defs = Definitions(
    jobs=[medical_warehouse_pipeline],
    schedules=[daily_schedule],
)

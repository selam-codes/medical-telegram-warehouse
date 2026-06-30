# Medical Telegram Warehouse

End-to-end ELT data product for Ethiopian medical/pharma Telegram channels — built for the
10 Academy Week 8 challenge. Raw messages and images are scraped from Telegram, landed in a
JSON data lake, loaded into PostgreSQL, and transformed into a dimensional star schema with dbt.
(Object detection enrichment, a FastAPI analytical layer, and Dagster orchestration are layered
on top in later tasks.)

## Project status

- [x] Task 1 — Scraping & data lake
- [x] Task 2 — dbt staging + star schema + tests
- [ ] Task 3 — YOLO image enrichment
- [ ] Task 4 — FastAPI analytical endpoints
- [ ] Task 5 — Dagster orchestration

## Project structure

```
medical-telegram-warehouse/
├── data/raw/
│   ├── telegram_messages/YYYY-MM-DD/{channel_name}.json
│   └── images/{channel_name}/{message_id}.jpg
├── logs/                         # scraper / loader run logs
├── src/
│   ├── scraper.py                # Task 1: Telethon scraper -> data lake
│   └── load_raw_to_postgres.py   # Task 2: data lake JSON -> raw.telegram_messages
├── medical_warehouse/            # dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/staging/           # stg_telegram_messages
│   ├── models/marts/             # dim_channels, dim_dates, fct_messages
│   └── tests/                    # custom data tests
├── api/                           # Task 4 (FastAPI) — stub for now
├── docs/interim_report.md
├── docker-compose.yml             # Postgres (+ app) container
├── Dockerfile
└── requirements.txt
```

## Setup

1. **Clone & create a virtualenv**

   ```shell
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Configure secrets**

   ```shell
   cp .env.example .env
   ```

   Fill in:
   - `TELEGRAM_API_ID` / `TELEGRAM_API_HASH` — from https://my.telegram.org
   - `TELEGRAM_PHONE` — the phone number tied to that Telegram account
   - `POSTGRES_*` — leave the defaults if using the bundled docker-compose Postgres

3. **Start PostgreSQL**

   ```shell
   docker compose up -d postgres
   ```

## Task 1 — Scrape Telegram

```shell
python src/scraper.py
```

The first run will prompt for the Telegram login code sent to your account (interactive,
one-time per session file). Subsequent runs reuse the saved `.session` file.

This populates:
- `data/raw/telegram_messages/YYYY-MM-DD/{channel}.json` — one file per channel/day, deduplicated by `message_id` across reruns
- `data/raw/images/{channel}/{message_id}.jpg`
- `logs/scraper_YYYY-MM-DD.log`

## Task 2 — Load & transform with dbt

1. **Load the data lake into Postgres**

   ```shell
   python src/load_raw_to_postgres.py
   ```

   Creates `raw.telegram_messages` and upserts every record from the data lake.

2. **Run dbt**

   ```shell
   cd medical_warehouse
   export DBT_PROFILES_DIR=$(pwd)
   dbt run
   dbt test
   dbt docs generate
   dbt docs serve
   ```

See [docs/interim_report.md](docs/interim_report.md) for the data lake structure, star schema
diagram, and data quality notes.

## Running tests

```shell
pytest tests/
```

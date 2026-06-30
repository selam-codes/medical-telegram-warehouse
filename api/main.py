"""
Analytical REST API for the Medical Telegram Warehouse.

Start:
    uvicorn api.main:app --reload --port 8000

Docs:
    http://localhost:8000/docs
"""

from typing import Optional
from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text

from api.database import get_db
from api.schemas import TopProduct, ChannelActivity, MessageResult, VisualContentStat

app = FastAPI(
    title="Medical Telegram Warehouse API",
    description="Analytical endpoints over the Ethiopian medical/pharma Telegram data warehouse.",
    version="1.0.0",
)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get(
    "/api/reports/top-products",
    response_model=list[TopProduct],
    summary="Top mentioned terms/products",
    description="Returns the most frequently occurring words across all channel messages, "
                "excluding common stop words. Useful for spotting top-mentioned drugs or products.",
)
def top_products(
    limit: int = Query(10, ge=1, le=100, description="Number of terms to return"),
    db: Session = Depends(get_db),
):
    sql = text("""
        with words as (
            select regexp_split_to_table(lower(message_text), '[^a-zA-Zሀ-፿]+') as term
            from public_marts.fct_messages
            where message_text is not null
        ),
        filtered as (
            select term, count(*) as mention_count
            from words
            where length(term) > 3
              and term not in (
                'this','that','with','have','from','they','will','been','were','when',
                'your','what','there','their','which','would','about','more','than',
                'also','some','into','just','like','only','over','such','then','very',
                'each','much','most','other','here','come','time','well','even',
                'back','good','want','know','take','need','made','many','give','best'
              )
            group by term
        )
        select term, mention_count
        from filtered
        order by mention_count desc
        limit :limit
    """)
    rows = db.execute(sql, {"limit": limit}).fetchall()
    return [{"term": r[0], "mention_count": r[1]} for r in rows]


@app.get(
    "/api/channels/{channel_name}/activity",
    response_model=ChannelActivity,
    summary="Channel posting activity",
    description="Returns posting stats and metadata for a single channel.",
)
def channel_activity(channel_name: str, db: Session = Depends(get_db)):
    sql = text("""
        select
            c.channel_name,
            c.channel_type,
            c.total_posts,
            round(c.avg_views::numeric, 1) as avg_views,
            count(f.message_key) filter (where f.has_image) as posts_with_images,
            c.first_post_date::text,
            c.last_post_date::text
        from public_marts.dim_channels c
        left join public_marts.fct_messages f on c.channel_key = f.channel_key
        where lower(c.channel_name) = lower(:channel_name)
        group by c.channel_name, c.channel_type, c.total_posts, c.avg_views,
                 c.first_post_date, c.last_post_date
    """)
    row = db.execute(sql, {"channel_name": channel_name}).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"Channel '{channel_name}' not found.")
    return {
        "channel_name": row[0], "channel_type": row[1], "total_posts": row[2],
        "avg_views": float(row[3]), "posts_with_images": row[4],
        "first_post_date": row[5], "last_post_date": row[6],
    }


@app.get(
    "/api/search/messages",
    response_model=list[MessageResult],
    summary="Search messages by keyword",
    description="Full-text keyword search across all scraped messages.",
)
def search_messages(
    query: str = Query(..., description="Keyword to search for"),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    sql = text("""
        select
            f.message_id,
            c.channel_name,
            d.full_date::text as message_date,
            f.message_text,
            f.views,
            f.forwards,
            f.has_image
        from public_marts.fct_messages f
        join public_marts.dim_channels c on f.channel_key = c.channel_key
        join public_marts.dim_dates   d on f.date_key    = d.date_key
        where lower(f.message_text) like lower(:query)
        order by f.views desc
        limit :limit
    """)
    rows = db.execute(sql, {"query": f"%{query}%", "limit": limit}).fetchall()
    return [
        {
            "message_id": r[0], "channel_name": r[1], "message_date": r[2],
            "message_text": r[3], "views": r[4], "forwards": r[5], "has_image": r[6],
        }
        for r in rows
    ]


@app.get(
    "/api/reports/visual-content",
    response_model=list[VisualContentStat],
    summary="Visual content statistics by channel",
    description="Returns image usage and YOLO-detected content category breakdown per channel.",
)
def visual_content(db: Session = Depends(get_db)):
    sql = text("""
        select
            c.channel_name,
            count(distinct f.message_key)                                           as total_messages,
            count(distinct f.message_key) filter (where f.has_image)               as messages_with_image,
            round(100.0 * count(distinct f.message_key) filter (where f.has_image)
                  / nullif(count(distinct f.message_key), 0), 1)                   as image_pct,
            mode() within group (order by fi.image_category)                        as most_common_category,
            count(fi.image_category) filter (where fi.image_category = 'promotional')     as promotional_count,
            count(fi.image_category) filter (where fi.image_category = 'product_display') as product_display_count,
            count(fi.image_category) filter (where fi.image_category = 'lifestyle')       as lifestyle_count,
            count(fi.image_category) filter (where fi.image_category = 'other')           as other_count
        from public_marts.dim_channels c
        left join public_marts.fct_messages f on c.channel_key = f.channel_key
        left join public_marts.fct_image_detections fi on f.message_key = fi.message_key
        group by c.channel_name
        order by total_messages desc
    """)
    rows = db.execute(sql).fetchall()
    return [
        {
            "channel_name": r[0], "total_messages": r[1], "messages_with_image": r[2],
            "image_pct": float(r[3] or 0), "most_common_category": r[4],
            "promotional_count": r[5], "product_display_count": r[6],
            "lifestyle_count": r[7], "other_count": r[8],
        }
        for r in rows
    ]

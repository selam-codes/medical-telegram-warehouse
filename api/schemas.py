from pydantic import BaseModel
from typing import Optional


class TopProduct(BaseModel):
    term: str
    mention_count: int


class ChannelActivity(BaseModel):
    channel_name: str
    channel_type: str
    total_posts: int
    avg_views: float
    posts_with_images: int
    first_post_date: str
    last_post_date: str


class MessageResult(BaseModel):
    message_id: int
    channel_name: str
    message_date: str
    message_text: Optional[str]
    views: int
    forwards: int
    has_image: bool


class VisualContentStat(BaseModel):
    channel_name: str
    total_messages: int
    messages_with_image: int
    image_pct: float
    most_common_category: Optional[str]
    promotional_count: int
    product_display_count: int
    lifestyle_count: int
    other_count: int

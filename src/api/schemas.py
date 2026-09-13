from datetime import date, datetime
import re
from typing import Any
from pydantic import (
    BaseModel,
    Field,
    field_validator,
    model_validator,
)


class WatchlistItemResponse(BaseModel):
    watchlist_id: int
    user_id: int
    username: str
    full_name: str | None = None
    ticker: str
    investment_thesis: str | None = None
    notes: str | None = None
    added_at: datetime
    updated_at: datetime


class WatchlistEntryResponse(BaseModel):
    watchlist_id: int
    user_id: int
    ticker: str
    investment_thesis: str | None = None
    notes: str | None = None
    added_at: datetime
    updated_at: datetime


class WatchlistItemCreate(BaseModel):
    ticker: str = Field(
        min_length=1,
        max_length=10,
    )

    investment_thesis: str | None = Field(
        default=None,
        max_length=2000,
    )

    notes: str | None = Field(
        default=None,
        max_length=2000,
    )

    @field_validator("ticker")
    @classmethod
    def validate_ticker(cls, value: str) -> str:
        ticker = value.strip().upper()

        if not re.fullmatch(r"[A-Z0-9.-]{1,10}", ticker):
            raise ValueError(
                "Ticker smije sadržati samo slova, brojeve, '.' i '-'."
            )

        return ticker


class WatchlistItemUpdate(BaseModel):
    investment_thesis: str | None = Field(
        default=None,
        max_length=2000,
    )

    notes: str | None = Field(
        default=None,
        max_length=2000,
    )

    @model_validator(mode="after")
    def validate_update(self):
        if self.investment_thesis is None and self.notes is None:
            raise ValueError(
                "Potrebno je poslati investment_thesis ili notes."
            )

        return self

class LatestStockMetricsResponse(BaseModel):
    ticker: str
    date: datetime | None = None

    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None

    volume: float | None = None
    vwap: float | None = None
    transactions: int | None = None

    previous_close: float | None = None
    price_change: float | None = None
    daily_return_pct: float | None = None

    moving_avg_7d: float | None = None
    moving_avg_30d: float | None = None

    volume_ratio: float | None = None
    annualized_volatility_30d_pct: float | None = None

    high_low_range_pct: float | None = None
    close_vs_vwap_pct: float | None = None

    daily_direction: str | None = None

    _gold_processed_at: datetime | None = None


class NewsSummaryResponse(BaseModel):
    ticker: str
    company_name: str | None = None
    data_as_of: date | None = None

    total_news_count: int | None = None
    news_7d_count: int | None = None
    news_30d_count: int | None = None

    positive_30d_count: int | None = None
    negative_30d_count: int | None = None
    neutral_30d_count: int | None = None
    unknown_30d_count: int | None = None
    scored_sentiment_30d_count: int | None = None

    sentiment_score_30d: float | None = None
    sentiment_coverage_30d_pct: float | None = None

    positive_share_30d_pct: float | None = None
    negative_share_30d_pct: float | None = None
    neutral_share_30d_pct: float | None = None

    dominant_sentiment_30d: str | None = None

    publisher_count_30d: int | None = None
    avg_news_per_day_30d: float | None = None
    news_volume_rank_30d: int | None = None

    latest_news_at: datetime | None = None
    latest_news_age_days: int | None = None

    latest_article_id: str | None = None
    latest_news_title: str | None = None
    latest_news_publisher: str | None = None
    latest_news_url: str | None = None
    latest_news_sentiment: str | None = None

    gold_processed_at: datetime | None = None

class NewsDailyAnalyticsResponse(BaseModel):
    ticker: str
    company_name: str | None = None
    published_date: date

    news_count: int | None = None

    positive_count: int | None = None
    negative_count: int | None = None
    neutral_count: int | None = None
    unknown_sentiment_count: int | None = None
    scored_sentiment_count: int | None = None

    sentiment_score: float | None = None
    sentiment_coverage_pct: float | None = None

    positive_share_pct: float | None = None
    negative_share_pct: float | None = None
    neutral_share_pct: float | None = None

    dominant_sentiment: str | None = None
    publisher_count: int | None = None

    latest_news_at: datetime | None = None
    gold_processed_at: datetime | None = None

class StockAnomalyResponse(BaseModel):
    ticker: str
    date: date

    close: float | None = None
    daily_return_pct: float | None = None
    daily_direction: str | None = None
    opening_gap_pct: float | None = None

    historical_volume_ratio: float | None = None

    return_zscore: float | None = None
    volume_zscore: float | None = None
    range_zscore: float | None = None
    gap_zscore: float | None = None
    transactions_zscore: float | None = None

    volatility_ratio: float | None = None
    rsi_14: float | None = None
    drawdown_60d_pct: float | None = None

    anomaly_signal_count: int | None = None
    anomaly_score: float | None = None
    anomaly_severity: str | None = None
    anomaly_type: str | None = None
    market_condition: str | None = None

    is_anomaly: bool

class AnomalySummaryResponse(BaseModel):
    ticker: str

    total_anomalies: int
    high_count: int
    medium_count: int
    low_count: int

    average_anomaly_score: float | None = None
    max_anomaly_score: float | None = None

    latest_anomaly_date: date
    latest_anomaly_score: float | None = None
    latest_anomaly_severity: str | None = None
    latest_anomaly_type: str | None = None
    latest_market_condition: str | None = None

class ResearchReportListItemResponse(BaseModel):
    report_id: int
    user_id: int
    ticker: str
    title: str
    question: str

    investment_thesis: str | None = None

    model: str
    thesis_assessment: str | None = None
    data_as_of: date | None = None

    created_at: datetime


class ResearchReportDetailResponse(BaseModel):
    report_id: int
    user_id: int
    username: str

    ticker: str
    title: str
    question: str

    investment_thesis: str | None = None

    model: str
    thesis_assessment: str | None = None
    data_as_of: date | None = None

    content: dict[str, Any]

    created_at: datetime

class ResearchRequest(BaseModel):
    user_id: int = Field(
        ge=1,
    )

    ticker: str = Field(
        min_length=1,
        max_length=10,
    )

    question: str = Field(
        min_length=1,
        max_length=4000,
    )

    @field_validator("ticker")
    @classmethod
    def validate_ticker(cls, value: str) -> str:
        ticker = value.strip().upper()

        if not re.fullmatch(
            r"[A-Z0-9.-]{1,10}",
            ticker,
        ):
            raise ValueError(
                "Ticker smije sadržati samo slova, "
                "brojeve, '.' i '-'."
            )

        return ticker

    @field_validator("question")
    @classmethod
    def validate_question(cls, value: str) -> str:
        question = value.strip()

        if not question:
            raise ValueError(
                "Research pitanje ne može biti prazno."
            )

        return question


class ResearchAgentResponse(BaseModel):
    report_id: int
    user_id: int

    username: str | None = None

    ticker: str
    question: str

    investment_thesis: str | None = None

    model: str
    data_as_of: str | None = None

    created_at: datetime

    grounding_validation: dict[str, Any]

    report: dict[str, Any]
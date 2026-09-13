from fastapi import (
    APIRouter,
    HTTPException,
    Query,
    Path,
    status,
)

from src.api.schemas import (
     AnomalySummaryResponse,
    LatestStockMetricsResponse,
    NewsDailyAnalyticsResponse,
    NewsSummaryResponse,
    StockAnomalyResponse,
)
from src.integration.analytics_service import (
    get_anomaly_summary,
    get_latest_stock_metrics,
    get_news_history,
    get_news_summary,
    get_stock_anomalies,
    get_stock_metrics_history,
)


router = APIRouter(
    prefix="/analytics",
    tags=["Analytics"],
)


@router.get(
    "/{ticker}/latest",
    response_model=LatestStockMetricsResponse,
    status_code=status.HTTP_200_OK,
)
def read_latest_stock_metrics(
    ticker: str = Path(
        ...,
        min_length=1,
        max_length=10,
        pattern=r"^[A-Za-z0-9.-]+$",
    ),
):
    """
    Returns the latest available Gold analytics for a ticker.
    """

    ticker = ticker.strip().upper()

    try:
        metrics = get_latest_stock_metrics(ticker)

    except Exception as exc:
        print("\n" + "=" * 80)
        print("DATABRICKS ANALYTICS ERROR")
        print("=" * 80)
        print(f"Exception type: {type(exc).__name__}")
        print(f"Exception message: {str(exc)}")
        print("=" * 80 + "\n")

        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Databricks analytics service "
                "is currently unavailable."
            ),
        ) from exc

    if metrics is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"No analytics data found for ticker {ticker}."
            ),
        )

    return metrics

@router.get(
    "/{ticker}/history",
    response_model=list[LatestStockMetricsResponse],
    status_code=status.HTTP_200_OK,
)
def read_stock_metrics_history(
    ticker: str = Path(
        ...,
        min_length=1,
        max_length=10,
        pattern=r"^[A-Za-z0-9.-]+$",
    ),
    limit: int = Query(
        default=30,
        ge=1,
        le=365,
    ),
):
    """
    Returns historical Gold analytics for a ticker.
    """

    ticker = ticker.strip().upper()

    try:
        metrics = get_stock_metrics_history(
            ticker=ticker,
            limit=limit,
        )

    except Exception as exc:
        print("\n" + "=" * 80)
        print("DATABRICKS ANALYTICS HISTORY ERROR")
        print("=" * 80)
        print(f"Exception type: {type(exc).__name__}")
        print(f"Exception message: {str(exc)}")
        print("=" * 80 + "\n")

        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Databricks analytics service "
                "is currently unavailable."
            ),
        ) from exc

    if not metrics:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"No analytics data found for ticker {ticker}."
            ),
        )

    return metrics

@router.get(
    "/{ticker}/news/summary",
    response_model=NewsSummaryResponse,
    status_code=status.HTTP_200_OK,
)
def read_news_summary(
    ticker: str = Path(
        ...,
        min_length=1,
        max_length=10,
        pattern=r"^[A-Za-z0-9.-]+$",
    ),
):
    """
    Returns Gold news summary analytics for a ticker.
    """

    ticker = ticker.strip().upper()

    try:
        summary = get_news_summary(ticker)

    except Exception as exc:
        print("\n" + "=" * 80)
        print("DATABRICKS NEWS ANALYTICS ERROR")
        print("=" * 80)
        print(f"Exception type: {type(exc).__name__}")
        print(f"Exception message: {str(exc)}")
        print("=" * 80 + "\n")

        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Databricks news analytics service "
                "is currently unavailable."
            ),
        ) from exc

    if summary is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No news analytics found for ticker {ticker}.",
        )

    return summary


@router.get(
    "/{ticker}/news/history",
    response_model=list[NewsDailyAnalyticsResponse],
    status_code=status.HTTP_200_OK,
)
def read_news_history(
    ticker: str = Path(
        ...,
        min_length=1,
        max_length=10,
        pattern=r"^[A-Za-z0-9.-]+$",
    ),
    limit: int = Query(
        default=30,
        ge=1,
        le=365,
    ),
):
    """
    Returns historical daily Gold news analytics for a ticker.
    """

    ticker = ticker.strip().upper()

    try:
        history = get_news_history(
            ticker=ticker,
            limit=limit,
        )

    except Exception as exc:
        print("\n" + "=" * 80)
        print("DATABRICKS NEWS HISTORY ERROR")
        print("=" * 80)
        print(f"Exception type: {type(exc).__name__}")
        print(f"Exception message: {str(exc)}")
        print("=" * 80 + "\n")

        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Databricks news history service "
                "is currently unavailable."
            ),
        ) from exc

    if not history:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No news history found for ticker {ticker}.",
        )

    return history

@router.get(
    "/{ticker}/anomalies",
    response_model=list[StockAnomalyResponse],
    status_code=status.HTTP_200_OK,
)
def read_stock_anomalies(
    ticker: str = Path(
        ...,
        min_length=1,
        max_length=10,
        pattern=r"^[A-Za-z0-9.-]+$",
    ),
    limit: int = Query(
        default=20,
        ge=1,
        le=100,
    ),
):
    """
    Returns detected Gold anomaly events for a ticker.
    """

    ticker = ticker.strip().upper()

    try:
        anomalies = get_stock_anomalies(
            ticker=ticker,
            limit=limit,
        )

    except Exception as exc:
        print("\n" + "=" * 80)
        print("DATABRICKS ANOMALY ANALYTICS ERROR")
        print("=" * 80)
        print(f"Exception type: {type(exc).__name__}")
        print(f"Exception message: {str(exc)}")
        print("=" * 80 + "\n")

        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Databricks anomaly analytics service "
                "is currently unavailable."
            ),
        ) from exc

    if not anomalies:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No anomalies found for ticker {ticker}.",
        )

    return anomalies

@router.get(
    "/{ticker}/anomalies/summary",
    response_model=AnomalySummaryResponse,
    status_code=status.HTTP_200_OK,
)
def read_anomaly_summary(
    ticker: str = Path(
        ...,
        min_length=1,
        max_length=10,
        pattern=r"^[A-Za-z0-9.-]+$",
    ),
):
    """
    Returns a summary of detected Gold anomalies for a ticker.
    """

    ticker = ticker.strip().upper()

    try:
        summary = get_anomaly_summary(ticker)

    except Exception as exc:
        print("\n" + "=" * 80)
        print("DATABRICKS ANOMALY SUMMARY ERROR")
        print("=" * 80)
        print(f"Exception type: {type(exc).__name__}")
        print(f"Exception message: {str(exc)}")
        print("=" * 80 + "\n")

        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Databricks anomaly summary service "
                "is currently unavailable."
            ),
        ) from exc

    if summary is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No anomalies found for ticker {ticker}.",
        )

    return summary
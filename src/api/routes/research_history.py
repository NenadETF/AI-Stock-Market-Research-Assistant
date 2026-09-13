from fastapi import (
    APIRouter,
    HTTPException,
    Query,
    Response,
    status,
)

from src.api.schemas import (
    ResearchReportDetailResponse,
    ResearchReportListItemResponse,
)

from src.lakebase.analysis_report_repository import (
    delete_analysis_report,
    get_analysis_report,
    list_analysis_reports,
)

from src.lakebase.user_repository import get_user_by_id


router = APIRouter(
    prefix="/users/{user_id}/research",
    tags=["Research History"],
)


# ============================================================
# LIST RESEARCH REPORTS
# ============================================================

@router.get(
    "",
    response_model=list[ResearchReportListItemResponse],
    status_code=status.HTTP_200_OK,
)
def read_research_history(
    user_id: int,
    ticker: str | None = Query(
        default=None,
        min_length=1,
        max_length=10,
        pattern=r"^[A-Za-z0-9.-]+$",
    ),
    limit: int = Query(
        default=50,
        ge=1,
        le=500,
    ),
):
    """
    Returns saved AI research reports for a user.

    Optionally filters reports by ticker.
    """

    user = get_user_by_id(user_id)

    if user is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User with ID={user_id} does not exist.",
        )

    if ticker is not None:
        ticker = ticker.strip().upper()

    return list_analysis_reports(
        user_id=user_id,
        ticker=ticker,
        limit=limit,
    )


# ============================================================
# GET ONE RESEARCH REPORT
# ============================================================

@router.get(
    "/{report_id}",
    response_model=ResearchReportDetailResponse,
    status_code=status.HTTP_200_OK,
)
def read_research_report(
    user_id: int,
    report_id: int,
):
    """
    Returns one saved AI research report.
    """

    user = get_user_by_id(user_id)

    if user is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User with ID={user_id} does not exist.",
        )

    report = get_analysis_report(report_id)

    if report is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Research report with ID={report_id} does not exist.",
        )

    # Report exists, but it must belong to this user.
    if report["user_id"] != user_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Research report with ID={report_id} does not exist.",
        )

    return report


# ============================================================
# DELETE RESEARCH REPORT
# ============================================================

@router.delete(
    "/{report_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
def delete_research_history_item(
    user_id: int,
    report_id: int,
):
    """
    Deletes one saved research report belonging to the user.
    """

    user = get_user_by_id(user_id)

    if user is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User with ID={user_id} does not exist.",
        )

    deleted = delete_analysis_report(
        report_id=report_id,
        user_id=user_id,
    )

    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Research report with ID={report_id} does not exist.",
        )

    return Response(
        status_code=status.HTTP_204_NO_CONTENT
    )
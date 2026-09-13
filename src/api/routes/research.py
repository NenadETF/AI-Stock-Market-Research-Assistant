from fastapi import (
    APIRouter,
    HTTPException,
    status,
)

from src.agent.research_agent import analyze_stock

from src.api.schemas import (
    ResearchAgentResponse,
    ResearchRequest,
)

from src.lakebase.user_repository import (
    get_user_by_id,
)

from src.lakebase.watchlist_repository import (
    get_watchlist_item,
)


router = APIRouter(
    prefix="/research",
    tags=["Research Agent"],
)


@router.post(
    "",
    response_model=ResearchAgentResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_research_report(
    payload: ResearchRequest,
):
    """
    Generates a personalized AI stock research report.

    The endpoint:
    - validates the user,
    - verifies that the ticker is on the user's watchlist,
    - builds the research context,
    - runs the Research Agent,
    - performs grounding validation,
    - saves the valid report to Lakebase,
    - returns the complete generated report.
    """

    # ==========================================================
    # 1. VALIDATE USER
    # ==========================================================

    user = get_user_by_id(
        payload.user_id
    )

    if user is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"User with ID={payload.user_id} "
                "does not exist."
            ),
        )

    # ==========================================================
    # 2. VALIDATE WATCHLIST ITEM
    # ==========================================================

    watchlist_item = get_watchlist_item(
        user_id=payload.user_id,
        ticker=payload.ticker,
    )

    if watchlist_item is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"Ticker {payload.ticker} is not "
                f"in the watchlist of user "
                f"{payload.user_id}."
            ),
        )

    # ==========================================================
    # 3. RUN RESEARCH AGENT
    # ==========================================================

    try:
        result = analyze_stock(
            user_id=payload.user_id,
            ticker=payload.ticker,
            question=payload.question,
        )

    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc

    except RuntimeError as exc:
        print("\n" + "=" * 80)
        print("RESEARCH AGENT VALIDATION ERROR")
        print("=" * 80)
        print(str(exc))
        print("=" * 80 + "\n")

        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc

    except Exception as exc:
        print("\n" + "=" * 80)
        print("RESEARCH AGENT SERVICE ERROR")
        print("=" * 80)
        print(f"Exception type: {type(exc).__name__}")
        print(f"Exception message: {str(exc)}")
        print("=" * 80 + "\n")

        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Research Agent service is currently "
                "unavailable."
            ),
        ) from exc

    # ==========================================================
    # 4. RETURN GENERATED AND SAVED REPORT
    # ==========================================================

    return result
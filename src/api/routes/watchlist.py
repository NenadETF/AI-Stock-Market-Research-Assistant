from fastapi import (
    APIRouter,
    HTTPException,
    Response,
    status,
)

from src.api.schemas import (
    WatchlistEntryResponse,
    WatchlistItemCreate,
    WatchlistItemResponse,
    WatchlistItemUpdate,
)

from src.lakebase.user_repository import get_user_by_id

from src.lakebase.watchlist_repository import (
    add_to_watchlist,
    get_watchlist,
    get_watchlist_item,
    remove_from_watchlist,
    update_watchlist_item,
)


router = APIRouter(
    prefix="/users/{user_id}/watchlist",
    tags=["Watchlist"],
)


# ============================================================
# GET WATCHLIST
# ============================================================

@router.get(
    "",
    response_model=list[WatchlistItemResponse],
    status_code=status.HTTP_200_OK,
)
def read_watchlist(user_id: int):
    """
    Returns all watchlist items for the selected user.
    """

    user = get_user_by_id(user_id)

    if user is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User with ID={user_id} does not exist.",
        )

    return get_watchlist(user_id)


# ============================================================
# ADD TO WATCHLIST
# ============================================================

@router.post(
    "",
    response_model=WatchlistEntryResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_watchlist_item(
    user_id: int,
    payload: WatchlistItemCreate,
):
    """
    Adds a ticker to the selected user's watchlist.
    """

    user = get_user_by_id(user_id)

    if user is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User with ID={user_id} does not exist.",
        )

    try:
        return add_to_watchlist(
            user_id=user_id,
            ticker=payload.ticker,
            investment_thesis=payload.investment_thesis,
            notes=payload.notes,
        )

    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        ) from exc


# ============================================================
# UPDATE WATCHLIST ITEM
# ============================================================

@router.put(
    "/{ticker}",
    response_model=WatchlistEntryResponse,
    status_code=status.HTTP_200_OK,
)
def update_watchlist(
    user_id: int,
    ticker: str,
    payload: WatchlistItemUpdate,
):
    """
    Updates investment thesis and/or notes for a watchlist item.
    """

    user = get_user_by_id(user_id)

    if user is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User with ID={user_id} does not exist.",
        )

    existing_item = get_watchlist_item(
        user_id=user_id,
        ticker=ticker,
    )

    if existing_item is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"Ticker {ticker.upper()} is not in "
                f"the watchlist of user {user_id}."
            ),
        )

    updated_item = update_watchlist_item(
        user_id=user_id,
        ticker=ticker,
        investment_thesis=payload.investment_thesis,
        notes=payload.notes,
    )

    if updated_item is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Watchlist item could not be updated.",
        )

    return updated_item


# ============================================================
# DELETE WATCHLIST ITEM
# ============================================================

@router.delete(
    "/{ticker}",
    status_code=status.HTTP_204_NO_CONTENT,
)
def delete_watchlist_item(
    user_id: int,
    ticker: str,
):
    """
    Removes a ticker from the selected user's watchlist.
    """

    user = get_user_by_id(user_id)

    if user is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User with ID={user_id} does not exist.",
        )

    deleted = remove_from_watchlist(
        user_id=user_id,
        ticker=ticker,
    )

    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"Ticker {ticker.upper()} is not in "
                f"the watchlist of user {user_id}."
            ),
        )

    return Response(
        status_code=status.HTTP_204_NO_CONTENT
    )
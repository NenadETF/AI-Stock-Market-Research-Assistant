from psycopg.errors import ForeignKeyViolation, UniqueViolation

from src.lakebase.connection import get_connection


def add_to_watchlist(
    user_id: int,
    ticker: str,
    investment_thesis: str | None = None,
    notes: str | None = None,
):
    ticker = ticker.strip().upper()

    if investment_thesis is not None:
        investment_thesis = investment_thesis.strip()

    if notes is not None:
        notes = notes.strip()

    sql = """
        INSERT INTO stock_research.watchlists (
            user_id,
            ticker,
            investment_thesis,
            notes
        )
        VALUES (%s, %s, %s, %s)
        RETURNING
            watchlist_id,
            user_id,
            ticker,
            investment_thesis,
            notes,
            added_at,
            updated_at;
    """

    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    sql,
                    (
                        user_id,
                        ticker,
                        investment_thesis,
                        notes,
                    ),
                )

                return cursor.fetchone()

    except UniqueViolation as exc:
        raise ValueError(
            f"Ticker {ticker} se već nalazi u watchlisti korisnika {user_id}."
        ) from exc

    except ForeignKeyViolation as exc:
        raise ValueError(
            f"Korisnik sa ID={user_id} ne postoji."
        ) from exc


def get_watchlist(user_id: int):
    sql = """
        SELECT
            w.watchlist_id,
            w.user_id,
            u.username,
            u.full_name,
            w.ticker,
            w.investment_thesis,
            w.notes,
            w.added_at,
            w.updated_at
        FROM stock_research.watchlists w
        JOIN stock_research.users u
            ON u.user_id = w.user_id
        WHERE w.user_id = %s
        ORDER BY w.ticker;
    """

    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, (user_id,))
            return cursor.fetchall()


def get_watchlist_item(
    user_id: int,
    ticker: str,
):
    ticker = ticker.strip().upper()

    sql = """
        SELECT
            watchlist_id,
            user_id,
            ticker,
            investment_thesis,
            notes,
            added_at,
            updated_at
        FROM stock_research.watchlists
        WHERE user_id = %s
          AND ticker = %s;
    """

    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                sql,
                (
                    user_id,
                    ticker,
                ),
            )

            return cursor.fetchone()


def update_watchlist_item(
    user_id: int,
    ticker: str,
    investment_thesis: str | None = None,
    notes: str | None = None,
):
    ticker = ticker.strip().upper()

    sql = """
        UPDATE stock_research.watchlists
        SET
            investment_thesis = COALESCE(%s, investment_thesis),
            notes = COALESCE(%s, notes)
        WHERE user_id = %s
          AND ticker = %s
        RETURNING
            watchlist_id,
            user_id,
            ticker,
            investment_thesis,
            notes,
            added_at,
            updated_at;
    """

    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                sql,
                (
                    investment_thesis,
                    notes,
                    user_id,
                    ticker,
                ),
            )

            return cursor.fetchone()


def remove_from_watchlist(
    user_id: int,
    ticker: str,
) -> bool:
    ticker = ticker.strip().upper()

    sql = """
        DELETE FROM stock_research.watchlists
        WHERE user_id = %s
          AND ticker = %s
        RETURNING watchlist_id;
    """

    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                sql,
                (
                    user_id,
                    ticker,
                ),
            )

            deleted = cursor.fetchone()

            return deleted is not None
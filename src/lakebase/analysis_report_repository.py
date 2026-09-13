from datetime import date

from psycopg.errors import ForeignKeyViolation
from psycopg.types.json import Jsonb

from src.lakebase.connection import get_connection


def save_analysis_report(
    user_id: int,
    ticker: str,
    title: str,
    question: str,
    content: dict,
    model: str,
    investment_thesis: str | None = None,
    thesis_assessment: str | None = None,
    data_as_of: str | date | None = None,
):
    """
    Saves a generated AI research report to Lakebase.
    """

    ticker = ticker.strip().upper()
    title = title.strip()
    question = question.strip()
    model = model.strip()

    if investment_thesis is not None:
        investment_thesis = investment_thesis.strip()

    if thesis_assessment is not None:
        thesis_assessment = thesis_assessment.strip().upper()

    if not ticker:
        raise ValueError("Ticker ne može biti prazan.")

    if not title:
        raise ValueError("Naslov izvještaja ne može biti prazan.")

    if not question:
        raise ValueError("Research pitanje ne može biti prazno.")

    if not model:
        raise ValueError("LLM model ne može biti prazan.")

    if not isinstance(content, dict):
        raise ValueError(
            "Sadržaj AI izvještaja mora biti Python dictionary."
        )

    sql = """
        INSERT INTO stock_research.analysis_reports (
            user_id,
            ticker,
            title,
            question,
            investment_thesis,
            model,
            thesis_assessment,
            data_as_of,
            content
        )
        VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        )
        RETURNING
            report_id,
            user_id,
            ticker,
            title,
            question,
            investment_thesis,
            model,
            thesis_assessment,
            data_as_of,
            content,
            created_at;
    """

    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    sql,
                    (
                        user_id,
                        ticker,
                        title,
                        question,
                        investment_thesis,
                        model,
                        thesis_assessment,
                        data_as_of,
                        Jsonb(content),
                    ),
                )

                return cursor.fetchone()

    except ForeignKeyViolation as exc:
        raise ValueError(
            f"Korisnik sa ID={user_id} ne postoji."
        ) from exc


def get_analysis_report(
    report_id: int,
):
    """
    Returns one saved AI report.
    """

    sql = """
        SELECT
            r.report_id,
            r.user_id,
            u.username,
            r.ticker,
            r.title,
            r.question,
            r.investment_thesis,
            r.model,
            r.thesis_assessment,
            r.data_as_of,
            r.content,
            r.created_at
        FROM stock_research.analysis_reports r
        JOIN stock_research.users u
            ON u.user_id = r.user_id
        WHERE r.report_id = %s;
    """

    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                sql,
                (report_id,),
            )

            return cursor.fetchone()


def list_analysis_reports(
    user_id: int,
    ticker: str | None = None,
    limit: int = 50,
):
    """
    Returns report history for one user.

    If ticker is supplied, only reports for that ticker
    are returned.
    """

    if limit < 1:
        raise ValueError(
            "Limit mora biti veći od 0."
        )

    if limit > 500:
        limit = 500

    # ==========================================================
    # FILTER BY TICKER
    # ==========================================================

    if ticker is not None:
        ticker = ticker.strip().upper()

        sql = """
            SELECT
                report_id,
                user_id,
                ticker,
                title,
                question,
                investment_thesis,
                model,
                thesis_assessment,
                data_as_of,
                created_at
            FROM stock_research.analysis_reports
            WHERE user_id = %s
              AND ticker = %s
            ORDER BY created_at DESC
            LIMIT %s;
        """

        params = (
            user_id,
            ticker,
            limit,
        )

    # ==========================================================
    # ALL USER REPORTS
    # ==========================================================

    else:
        sql = """
            SELECT
                report_id,
                user_id,
                ticker,
                title,
                question,
                investment_thesis,
                model,
                thesis_assessment,
                data_as_of,
                created_at
            FROM stock_research.analysis_reports
            WHERE user_id = %s
            ORDER BY created_at DESC
            LIMIT %s;
        """

        params = (
            user_id,
            limit,
        )

    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                sql,
                params,
            )

            return cursor.fetchall()
def count_analysis_reports(
    user_id: int,
) -> int:
    """
    Counts saved reports for one user.
    """

    sql = """
        SELECT COUNT(*) AS report_count
        FROM stock_research.analysis_reports
        WHERE user_id = %s;
    """

    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                sql,
                (user_id,),
            )

            result = cursor.fetchone()

            return result["report_count"]


def delete_analysis_report(
    report_id: int,
    user_id: int,
) -> bool:
    """
    Deletes a report only if it belongs to the supplied user.
    """

    sql = """
        DELETE FROM stock_research.analysis_reports
        WHERE report_id = %s
          AND user_id = %s
        RETURNING report_id;
    """

    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                sql,
                (
                    report_id,
                    user_id,
                ),
            )

            deleted = cursor.fetchone()

            return deleted is not None
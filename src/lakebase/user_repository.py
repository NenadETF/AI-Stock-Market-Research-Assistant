from psycopg.errors import UniqueViolation

from src.lakebase.connection import get_connection


def create_user(
    username: str,
    email: str,
    full_name: str | None = None,
):
    username = username.strip()
    email = email.strip().lower()

    if full_name is not None:
        full_name = full_name.strip()

    sql = """
        INSERT INTO stock_research.users (
            username,
            email,
            full_name
        )
        VALUES (%s, %s, %s)
        RETURNING
            user_id,
            username,
            email,
            full_name,
            is_active,
            created_at,
            updated_at;
    """

    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    sql,
                    (
                        username,
                        email,
                        full_name,
                    ),
                )

                return cursor.fetchone()

    except UniqueViolation as exc:
        raise ValueError(
            "Korisnik sa tim username-om ili email adresom već postoji."
        ) from exc


def get_user_by_id(user_id: int):
    sql = """
        SELECT
            user_id,
            username,
            email,
            full_name,
            is_active,
            created_at,
            updated_at
        FROM stock_research.users
        WHERE user_id = %s;
    """

    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, (user_id,))
            return cursor.fetchone()


def get_user_by_username(username: str):
    sql = """
        SELECT
            user_id,
            username,
            email,
            full_name,
            is_active,
            created_at,
            updated_at
        FROM stock_research.users
        WHERE username = %s;
    """

    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                sql,
                (username.strip(),),
            )

            return cursor.fetchone()


def list_users():
    sql = """
        SELECT
            user_id,
            username,
            email,
            full_name,
            is_active,
            created_at,
            updated_at
        FROM stock_research.users
        ORDER BY user_id;
    """

    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchall()

def update_user(
    user_id: int,
    email: str | None = None,
    full_name: str | None = None,
    is_active: bool | None = None,
):
    if email is not None:
        email = email.strip().lower()

    if full_name is not None:
        full_name = full_name.strip()

    sql = """
        UPDATE stock_research.users
        SET
            email = COALESCE(%s, email),
            full_name = COALESCE(%s, full_name),
            is_active = COALESCE(%s, is_active)
        WHERE user_id = %s
        RETURNING
            user_id,
            username,
            email,
            full_name,
            is_active,
            created_at,
            updated_at;
    """

    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    sql,
                    (
                        email,
                        full_name,
                        is_active,
                        user_id,
                    ),
                )

                return cursor.fetchone()

    except UniqueViolation as exc:
        raise ValueError(
            "Drugi korisnik već koristi tu email adresu."
        ) from exc


def delete_user(user_id: int) -> bool:
    sql = """
        DELETE FROM stock_research.users
        WHERE user_id = %s
        RETURNING user_id;
    """

    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, (user_id,))

            deleted = cursor.fetchone()

            return deleted is not None
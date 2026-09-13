import os

import psycopg
from dotenv import load_dotenv
from psycopg.rows import dict_row


load_dotenv()


def get_connection():
    host = os.getenv("LAKEBASE_HOST")
    port = os.getenv("LAKEBASE_PORT", "5432")
    database = os.getenv("LAKEBASE_DATABASE")
    user = os.getenv("LAKEBASE_USER")
    password = os.getenv("LAKEBASE_PASSWORD")
    sslmode = os.getenv("LAKEBASE_SSLMODE", "require")

    required = {
        "LAKEBASE_HOST": host,
        "LAKEBASE_DATABASE": database,
        "LAKEBASE_USER": user,
        "LAKEBASE_PASSWORD": password,
    }

    missing = [
        key
        for key, value in required.items()
        if not value
    ]

    if missing:
        raise RuntimeError(
            "Nedostaju Lakebase varijable: "
            + ", ".join(missing)
        )

    return psycopg.connect(
        host=host,
        port=int(port),
        dbname=database,
        user=user,
        password=password,
        sslmode=sslmode,
        row_factory=dict_row,
        connect_timeout=15,
    )
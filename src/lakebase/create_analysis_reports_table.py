from src.lakebase.connection import get_connection


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_research.analysis_reports (
    report_id BIGSERIAL PRIMARY KEY,

    user_id BIGINT NOT NULL,

    ticker VARCHAR(16) NOT NULL,

    title VARCHAR(255) NOT NULL,

    question TEXT NOT NULL,

    investment_thesis TEXT,

    model VARCHAR(150) NOT NULL,

    thesis_assessment VARCHAR(32),

    data_as_of DATE,

    content JSONB NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_analysis_reports_user
        FOREIGN KEY (user_id)
        REFERENCES stock_research.users(user_id)
        ON DELETE CASCADE
);
"""


CREATE_INDEXES_SQL = [
    """
    CREATE INDEX IF NOT EXISTS idx_analysis_reports_user
    ON stock_research.analysis_reports(user_id);
    """,

    """
    CREATE INDEX IF NOT EXISTS idx_analysis_reports_ticker
    ON stock_research.analysis_reports(ticker);
    """,

    """
    CREATE INDEX IF NOT EXISTS idx_analysis_reports_user_ticker
    ON stock_research.analysis_reports(user_id, ticker);
    """,

    """
    CREATE INDEX IF NOT EXISTS idx_analysis_reports_created_at
    ON stock_research.analysis_reports(created_at DESC);
    """,
]


def main():
    print("=" * 90)
    print("LAKEBASE - CREATE ANALYSIS REPORTS TABLE")
    print("=" * 90)

    with get_connection() as conn:
        with conn.cursor() as cursor:

            cursor.execute(CREATE_TABLE_SQL)

            for sql in CREATE_INDEXES_SQL:
                cursor.execute(sql)

    print()
    print("[PASS] stock_research.analysis_reports is ready")
    print()
    print("=" * 90)


if __name__ == "__main__":
    main()
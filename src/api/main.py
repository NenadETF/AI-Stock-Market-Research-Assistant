import psycopg

from fastapi.middleware.cors import CORSMiddleware

from fastapi import (
    FastAPI,
    Request,
    status,
)

from fastapi.responses import JSONResponse


from src.api.routes.analytics import (
    router as analytics_router,
)

from src.api.routes.research import (
    router as research_router,
)

from src.api.routes.research_history import (
    router as research_history_router,
)

from src.api.routes.watchlist import (
    router as watchlist_router,
)

from src.api.routes.system import (
    router as system_router,
)


# ============================================================
# APPLICATION
# ============================================================

app = FastAPI(
    title=(
        "AI Stock Market Research Assistant API"
    ),
    description=(
        "Application API for watchlists, stock analytics, "
        "research history, system status and the "
        "AI Research Agent."
    ),
    version="1.0.0",
)


# ============================================================
# CORS
# ============================================================

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=[
        "*"
    ],
    allow_headers=[
        "*"
    ],
)


# ============================================================
# GLOBAL ERROR HANDLERS
# ============================================================

@app.exception_handler(
    psycopg.OperationalError
)
async def lakebase_connection_error_handler(
    request: Request,
    exc: psycopg.OperationalError,
):
    """
    Converts Lakebase connection/authentication failures
    into a controlled HTTP 503 response.
    """

    print(
        "\n"
        + "=" * 80
    )

    print(
        "LAKEBASE CONNECTION ERROR"
    )

    print(
        "=" * 80
    )

    print(
        f"Request: "
        f"{request.method} "
        f"{request.url.path}"
    )

    print(
        f"Exception: {str(exc)}"
    )

    print(
        "=" * 80
        + "\n"
    )


    return JSONResponse(
        status_code=(
            status
            .HTTP_503_SERVICE_UNAVAILABLE
        ),
        content={
            "detail": (
                "Lakebase service is "
                "currently unavailable."
            )
        },
    )


# ============================================================
# ROUTERS
# ============================================================

app.include_router(
    watchlist_router
)

app.include_router(
    analytics_router
)

app.include_router(
    research_history_router
)

app.include_router(
    research_router
)

app.include_router(
    system_router
)


# ============================================================
# BASIC SERVICE ENDPOINTS
# ============================================================

@app.get("/")
def root():

    return {
        "application": (
            "AI Stock Market Research Assistant"
        ),
        "service": "API",
        "status": "running",
    }


@app.get("/health")
def health_check():

    return {
        "status": "ok"
    }
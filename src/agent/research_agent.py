import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from src.agent.grounding_validator import (
    validate_report_against_context,
)
from src.agent.llm_client import LLMClient
from src.agent.prompts import (
    RESEARCH_AGENT_SYSTEM_PROMPT,
    build_research_prompt,
)
from src.integration.research_context_service import (
    get_research_context,
)
from src.lakebase.analysis_report_repository import (
    save_analysis_report,
)


class ResearchAgent:
    """
    AI Stock Market Research Agent.

    Responsibilities:
    1. Load personalized research context.
    2. Validate the context.
    3. Extract the user's investment thesis.
    4. Build a compact LLM context.
    5. Generate a grounded research report.
    6. Validate generated output.
    7. Perform deterministic grounding validation.
    8. Save only valid reports to Lakebase.
    """

    def __init__(
        self,
        llm_client: LLMClient | None = None,
    ):
        self.llm = llm_client or LLMClient()

    # ==========================================================
    # JSON SERIALIZATION
    # ==========================================================

    @staticmethod
    def _json_serializer(value: Any):
        """
        Converts SQL / Databricks values into JSON-safe values.
        """

        if isinstance(value, (date, datetime)):
            return value.isoformat()

        if isinstance(value, Decimal):
            return float(value)

        return str(value)

    # ==========================================================
    # COMPACT LLM CONTEXT
    # ==========================================================

    def _serialize_context(
        self,
        context: dict[str, Any],
    ) -> str:
        """
        Builds a compact Research Context for the LLM.

        Only information required for stock analysis is sent
        to the model. This reduces token usage and keeps the
        LLM grounded in application data.
        """

        compact_context = {
            "user": {
                "user_id": (
                    context
                    .get("user", {})
                    .get("user_id")
                ),
                "username": (
                    context
                    .get("user", {})
                    .get("username")
                ),
            },

            "watchlist": {
                "ticker": (
                    context
                    .get("watchlist", {})
                    .get("ticker")
                ),
                "investment_thesis": (
                    context
                    .get("watchlist", {})
                    .get("investment_thesis")
                ),
                "notes": (
                    context
                    .get("watchlist", {})
                    .get("notes")
                ),
            },

            "company": context.get(
                "company"
            ),

            "market": context.get(
                "market"
            ),

            "performance": context.get(
                "performance"
            ),

            "technical": context.get(
                "technical"
            ),

            "news": {
                "summary": (
                    context
                    .get("news", {})
                    .get("summary")
                ),

                "recent_daily": (
                    context
                    .get("news", {})
                    .get("recent_daily", [])[:7]
                ),
            },

            "anomalies": {
                "recent": (
                    context
                    .get("anomalies", {})
                    .get("recent", [])[:5]
                ),

                "news_context": (
                    context
                    .get("anomalies", {})
                    .get("news_context", [])[:5]
                ),
            },
        }

        return json.dumps(
            compact_context,
            ensure_ascii=False,
            default=self._json_serializer,
            separators=(",", ":"),
        )

    # ==========================================================
    # CONTEXT VALIDATION
    # ==========================================================

    @staticmethod
    def _validate_context(
        context: dict[str, Any],
        user_id: int,
        ticker: str,
    ):
        """
        Validates that Research Context belongs to the expected
        user and ticker and contains all required sections.
        """

        required_sections = [
            "user",
            "watchlist",
            "company",
            "market",
            "performance",
            "technical",
            "news",
            "anomalies",
        ]

        missing = [
            section
            for section in required_sections
            if section not in context
        ]

        if missing:
            raise RuntimeError(
                "Research context is missing required sections: "
                + ", ".join(missing)
            )

        context_user_id = (
            context
            .get("user", {})
            .get("user_id")
        )

        if context_user_id != user_id:
            raise RuntimeError(
                "Research context contains unexpected user."
            )

        context_ticker = (
            str(
                context
                .get("watchlist", {})
                .get("ticker", "")
            )
            .strip()
            .upper()
        )

        if context_ticker != ticker:
            raise RuntimeError(
                "Research context ticker mismatch: "
                f"expected {ticker}, "
                f"received {context_ticker}"
            )

    # ==========================================================
    # LLM REPORT VALIDATION
    # ==========================================================

    @staticmethod
    def _validate_report(
        report: dict[str, Any],
    ):
        """
        Validates the structure returned by the LLM before
        deterministic grounding checks are performed.
        """

        if not isinstance(report, dict):
            raise RuntimeError(
                "LLM report must be a JSON object."
            )

        required_fields = [
            "ticker",
            "data_as_of",
            "executive_summary",
            "market_analysis",
            "technical_analysis",
            "news_analysis",
            "anomaly_analysis",
            "thesis_alignment",
            "key_catalysts",
            "key_risks",
            "data_limitations",
            "conclusion",
            "disclaimer",
        ]

        missing = [
            field
            for field in required_fields
            if field not in report
        ]

        if missing:
            raise RuntimeError(
                "LLM report is missing required fields: "
                + ", ".join(missing)
            )

        thesis_alignment = report.get(
            "thesis_alignment"
        )

        if not isinstance(
            thesis_alignment,
            dict,
        ):
            raise RuntimeError(
                "thesis_alignment must be an object."
            )

        required_thesis_fields = [
            "assessment",
            "supporting_evidence",
            "contradicting_evidence",
        ]

        missing_thesis_fields = [
            field
            for field in required_thesis_fields
            if field not in thesis_alignment
        ]

        if missing_thesis_fields:
            raise RuntimeError(
                "thesis_alignment is missing fields: "
                + ", ".join(
                    missing_thesis_fields
                )
            )

        valid_assessments = {
            "SUPPORTS",
            "PARTIALLY_SUPPORTS",
            "NEUTRAL",
            "PARTIALLY_CONTRADICTS",
            "CONTRADICTS",
            "INSUFFICIENT_DATA",
        }

        assessment = thesis_alignment.get(
            "assessment"
        )

        if assessment not in valid_assessments:
            raise RuntimeError(
                "Invalid thesis assessment returned "
                f"by LLM: {assessment}"
            )

        list_fields = [
            "key_catalysts",
            "key_risks",
            "data_limitations",
        ]

        for field in list_fields:
            if not isinstance(
                report.get(field),
                list,
            ):
                raise RuntimeError(
                    f"{field} must be a list."
                )

        if not isinstance(
            thesis_alignment.get(
                "supporting_evidence"
            ),
            list,
        ):
            raise RuntimeError(
                "supporting_evidence must be a list."
            )

        if not isinstance(
            thesis_alignment.get(
                "contradicting_evidence"
            ),
            list,
        ):
            raise RuntimeError(
                "contradicting_evidence must be a list."
            )

    # ==========================================================
    # MAIN AGENT METHOD
    # ==========================================================

    def analyze(
        self,
        user_id: int,
        ticker: str,
        question: str,
    ) -> dict[str, Any]:
        """
        Generates, validates and permanently saves a
        personalized stock research report.
        """

        ticker = ticker.strip().upper()
        question = question.strip()

        if not ticker:
            raise ValueError(
                "Ticker cannot be empty."
            )

        if not question:
            raise ValueError(
                "Research question cannot be empty."
            )

        # ======================================================
        # 1. LOAD RESEARCH CONTEXT
        # ======================================================

        context = get_research_context(
            user_id=user_id,
            ticker=ticker,
        )

        # ======================================================
        # 2. VALIDATE CONTEXT
        # ======================================================

        self._validate_context(
            context=context,
            user_id=user_id,
            ticker=ticker,
        )

        # ======================================================
        # 3. LOAD INVESTMENT THESIS
        # ======================================================

        investment_thesis = (
            context
            .get("watchlist", {})
            .get("investment_thesis")
        )

        # ======================================================
        # 4. DETERMINE MARKET DATA FRESHNESS
        # ======================================================

        market_date = None

        if context.get("market"):
            market_date = (
                context
                .get("market", {})
                .get("date")
            )

        if market_date is not None:
            market_date = str(
                market_date
            )

        # ======================================================
        # 5. CREATE COMPACT LLM CONTEXT
        # ======================================================

        context_json = (
            self._serialize_context(
                context
            )
        )

        # ======================================================
        # 6. BUILD GROUNDED PROMPT
        # ======================================================

        user_prompt = build_research_prompt(
            ticker=ticker,
            question=question,
            research_context_json=context_json,
            investment_thesis=investment_thesis,
        )

        # ======================================================
        # 7. GENERATE LLM REPORT
        # ======================================================

        report = self.llm.generate_json(
            system_prompt=(
                RESEARCH_AGENT_SYSTEM_PROMPT
            ),
            user_prompt=user_prompt,

            # Chosen to remain inside Groq Free /
            # On-Demand token limits.
            max_tokens=1800,
        )

        # ======================================================
        # 8. VALIDATE REPORT STRUCTURE
        # ======================================================

        self._validate_report(
            report
        )

        # ======================================================
        # 9. DETERMINISTIC GROUNDING VALIDATION
        # ======================================================

        grounding_validation = (
            validate_report_against_context(
                report=report,
                context=context,
            )
        )

        if (
                grounding_validation["status"]
                != "PASS"
            ):
                details = "\n".join(
                    f"- {error}"
                    for error
                    in grounding_validation["errors"]
                )

                print()
                print("=" * 110)
                print("GROUNDING VALIDATION FAILED")
                print("=" * 110)

                print("\nErrors:")

                for error in grounding_validation.get(
                    "errors",
                    [],
                ):
                    print(
                        f"  - {error}"
                    )

                print("\nDiagnostics:")

                for warning in grounding_validation.get(
                    "warnings",
                    [],
                ):
                    print(
                        f"  - {warning}"
                    )

                print("=" * 110)
                print()

                raise RuntimeError(
                    "Grounding validation failed. "
                    "The AI report will NOT be saved.\n"
                    f"{details}"
                )

        # ======================================================
        # 10. PREPARE REPORT METADATA
        # ======================================================

        thesis_assessment = (
            report
            .get("thesis_alignment", {})
            .get("assessment")
        )

        report_title = (
            f"{ticker} AI Research Report"
        )

        # ======================================================
        # 11. SAVE VALID REPORT TO LAKEBASE
        # ======================================================

        saved_report = (
            save_analysis_report(
                user_id=user_id,
                ticker=ticker,
                title=report_title,
                question=question,
                investment_thesis=investment_thesis,
                model=self.llm.model,
                thesis_assessment=(
                    thesis_assessment
                ),
                data_as_of=market_date,
                content=report,
            )
        )

        if not saved_report:
            raise RuntimeError(
                "Analysis report was generated but "
                "could not be saved to Lakebase."
            )

        # ======================================================
        # 12. RETURN COMPLETE AGENT RESULT
        # ======================================================

        return {
            "report_id": (
                saved_report["report_id"]
            ),

            "user_id": user_id,

            "username": (
                context
                .get("user", {})
                .get("username")
            ),

            "ticker": ticker,

            "question": question,

            "investment_thesis": (
                investment_thesis
            ),

            "model": self.llm.model,

            "data_as_of": (
                market_date
            ),

            "created_at": (
                saved_report["created_at"]
            ),

            "grounding_validation": (
                grounding_validation
            ),

            "report": report,
        }


# ==============================================================
# CONVENIENCE FUNCTION
# ==============================================================


def analyze_stock(
    user_id: int,
    ticker: str,
    question: str,
) -> dict[str, Any]:
    """
    Convenience function used by tests and later by the
    application/service layer.
    """

    agent = ResearchAgent()

    return agent.analyze(
        user_id=user_id,
        ticker=ticker,
        question=question,
    )
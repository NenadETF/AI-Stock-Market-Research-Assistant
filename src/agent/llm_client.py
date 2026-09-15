import json
import os
from typing import Any

from openai import OpenAI


DEFAULT_MODEL = "openai/gpt-oss-120b"
GROQ_BASE_URL = "https://api.groq.com/openai/v1"


class LLMClient:
    """
    Provider-neutral LLM client used by the Research Agent.

    Current provider:
        Groq via OpenAI-compatible API.
    """

    def __init__(
        self,
        model: str | None = None,
        temperature: float = 0.1,
    ):
        api_key = os.getenv("GROQ_API_KEY")

        if not api_key:
            raise RuntimeError(
                "GROQ_API_KEY environment variable is not configured."
            )

        self.model = (
            model
            or os.getenv(
                "GROQ_MODEL",
                DEFAULT_MODEL,
            )
        )

        self.temperature = temperature

        self.client = OpenAI(
            api_key=api_key,
            base_url=GROQ_BASE_URL,
        )

    # ==========================================================
    # STANDARD TEXT GENERATION
    # ==========================================================

    def generate(
        self,
        system_prompt: str,
        user_prompt: str,
        max_tokens: int = 1800,
    ) -> str:
        """
        Generates a normal text response.
        """

        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {
                    "role": "system",
                    "content": system_prompt,
                },
                {
                    "role": "user",
                    "content": user_prompt,
                },
            ],
            temperature=self.temperature,
            max_tokens=max_tokens,
        )

        content = response.choices[0].message.content

        if not content:
            raise RuntimeError(
                "LLM returned an empty response."
            )

        return content.strip()

    # ==========================================================
    # STRUCTURED JSON GENERATION
    # ==========================================================

    def generate_json(
        self,
        system_prompt: str,
        user_prompt: str,
        max_tokens: int = 1800,
    ) -> dict[str, Any]:
        """
        Generates a structured JSON response.

        JSON Object Mode is used to reduce malformed responses.

        Low reasoning effort reduces reasoning-token usage and leaves
        more of the available token budget for the actual report.
        """

        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {
                    "role": "system",
                    "content": system_prompt,
                },
                {
                    "role": "user",
                    "content": user_prompt,
                },
            ],
            temperature=self.temperature,

            # GPT-OSS reasoning configuration.
            reasoning_effort="low",

            # Force JSON object output.
            response_format={
                "type": "json_object",
            },

            max_tokens=max_tokens,
        )

        if not response.choices:
            raise RuntimeError(
                "LLM returned no choices."
            )

        choice = response.choices[0]

        content = choice.message.content

        if not content:
            raise RuntimeError(
                "LLM returned an empty JSON response."
            )

        # Explicitly detect truncated model output.
        if choice.finish_reason == "length":
            raise RuntimeError(
                "LLM response was truncated because the output "
                "token limit was reached."
            )

        cleaned = content.strip()

        # Defensive cleanup in case a model still adds Markdown fences.
        if cleaned.startswith("```"):
            lines = cleaned.splitlines()

            if lines and lines[0].startswith("```"):
                lines = lines[1:]

            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]

            cleaned = "\n".join(lines).strip()

            if cleaned.lower().startswith("json"):
                cleaned = cleaned[4:].strip()

        try:
            parsed = json.loads(cleaned)

        except json.JSONDecodeError as exc:
            raise RuntimeError(
                "LLM response is not valid JSON.\n\n"
                f"Finish reason: {choice.finish_reason}\n"
                f"Raw response:\n{content}"
            ) from exc

        if not isinstance(parsed, dict):
            raise RuntimeError(
                "LLM JSON response must be an object."
            )

        return parsed
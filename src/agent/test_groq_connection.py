import os

from openai import OpenAI


MODEL_NAME = "openai/gpt-oss-120b"


def main():
    print("=" * 90)
    print("GROQ LLM CONNECTION TEST")
    print("=" * 90)

    api_key = os.getenv("GROQ_API_KEY")

    if not api_key:
        raise RuntimeError(
            "GROQ_API_KEY environment variable is not configured."
        )

    print(f"\nModel: {MODEL_NAME}")
    print("Interface: Groq / OpenAI-compatible API")
    print("[PASS] GROQ_API_KEY detected")

    try:
        client = OpenAI(
            api_key=api_key,
            base_url="https://api.groq.com/openai/v1",
        )

        print("[PASS] OpenAI-compatible client initialized")
        print("\nSending test request...\n")

        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a stock market research assistant. "
                        "Use only supplied information when analyzing stocks. "
                        "Do not invent market data."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        "In one sentence, explain the difference between "
                        "stock price and market capitalization."
                    ),
                },
            ],
            temperature=0.1,
            max_tokens=150,
        )

        answer = response.choices[0].message.content

        print("=" * 90)
        print("[PASS] LLM RESPONSE RECEIVED")
        print("=" * 90)
        print()
        print(answer)

        if response.usage:
            print()
            print("-" * 90)
            print("TOKEN USAGE")
            print("-" * 90)
            print(f"Prompt tokens:     {response.usage.prompt_tokens}")
            print(f"Completion tokens: {response.usage.completion_tokens}")
            print(f"Total tokens:      {response.usage.total_tokens}")

        print()
        print("=" * 90)
        print("[PASS] GROQ LLM CONNECTION WORKS")
        print("=" * 90)

    except Exception as exc:
        print()
        print("=" * 90)
        print("[FAIL] GROQ LLM CONNECTION")
        print("=" * 90)
        print(f"Exception type: {type(exc).__name__}")
        print(f"Message: {exc}")
        print("=" * 90)

        raise


if __name__ == "__main__":
    main()
from llm_client import LLMClient


def main():
    print("=" * 90)
    print("LLM CLIENT TEST")
    print("=" * 90)

    client = LLMClient()

    response = client.generate(
        system_prompt=(
            "You are a stock market research assistant. "
            "Answer concisely."
        ),
        user_prompt=(
            "Explain in one sentence what a bullish stock trend means."
        ),
        max_tokens=150,
    )

    print("\nModel:")
    print(client.model)

    print("\nResponse:")
    print(response)

    print("\n" + "=" * 90)
    print("[PASS] LLM CLIENT WORKS")
    print("=" * 90)


if __name__ == "__main__":
    main()
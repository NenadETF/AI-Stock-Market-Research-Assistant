from databricks.sdk import WorkspaceClient
from openai import OpenAI


MODEL_NAME = "system.ai.claude-sonnet-4-5"


def main():
    print("=" * 90)
    print("DATABRICKS LLM CONNECTION TEST")
    print("=" * 90)

    print(f"\nModel service: {MODEL_NAME}")
    print("Interface: Unity AI Gateway / OpenAI-compatible API")
    print("\nConnecting to Databricks...\n")

    try:
        # ----------------------------------------------------------
        # 1. Use the same Databricks authentication already used
        #    by the rest of our project / VS Code Databricks setup.
        # ----------------------------------------------------------
        workspace = WorkspaceClient()

        host = workspace.config.host.rstrip("/")
        auth_headers = workspace.config.authenticate()

        authorization = auth_headers.get("Authorization", "")

        if not authorization.startswith("Bearer "):
            raise RuntimeError(
                "Databricks authentication did not return a Bearer token."
            )

        token = authorization.split(" ", 1)[1]

        print(f"[PASS] Databricks authentication works")
        print(f"Workspace: {host}")

        # ----------------------------------------------------------
        # 2. OpenAI-compatible Unity AI Gateway client
        # ----------------------------------------------------------
        client = OpenAI(
            api_key=token,
            base_url=f"{host}/ai-gateway/mlflow/v1",
        )

        print("[PASS] OpenAI-compatible client initialized")
        print(f"\nSending request to {MODEL_NAME}...\n")

        # ----------------------------------------------------------
        # 3. Test LLM request
        # ----------------------------------------------------------
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a stock market research assistant. "
                        "Respond clearly and concisely."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        "In one sentence, explain the difference "
                        "between stock price and market capitalization."
                    ),
                },
            ],
            max_tokens=150,
            temperature=0.1,
        )

        answer = response.choices[0].message.content

        print("=" * 90)
        print("[PASS] LLM RESPONSE RECEIVED")
        print("=" * 90)
        print()
        print(answer)
        print()

        if response.usage:
            print("-" * 90)
            print("TOKEN USAGE")
            print("-" * 90)
            print(f"Prompt tokens:     {response.usage.prompt_tokens}")
            print(f"Completion tokens: {response.usage.completion_tokens}")
            print(f"Total tokens:      {response.usage.total_tokens}")

        print()
        print("=" * 90)
        print("[PASS] DATABRICKS UNITY AI GATEWAY WORKS")
        print("=" * 90)

    except Exception as exc:
        print()
        print("=" * 90)
        print("[FAIL] LLM CONNECTION")
        print("=" * 90)
        print(f"Exception type: {type(exc).__name__}")
        print(f"Message: {exc}")
        print("=" * 90)

        raise


if __name__ == "__main__":
    main()
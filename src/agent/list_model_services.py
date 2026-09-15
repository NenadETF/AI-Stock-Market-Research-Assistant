from databricks.sdk import WorkspaceClient
import requests


def main():
    print("=" * 100)
    print("DATABRICKS UNITY AI GATEWAY - AVAILABLE MODEL SERVICES")
    print("=" * 100)

    workspace = WorkspaceClient()

    host = workspace.config.host.rstrip("/")
    auth_headers = workspace.config.authenticate()

    print(f"\nWorkspace: {host}")
    print("[PASS] Databricks authentication works")

    url = f"{host}/api/2.1/unity-catalog/model-services"

    params = {
        "parent": "schemas/system.ai",
        "page_size": 100,
        "view": "BASIC",
    }

    print("\nSearching system.ai model services...\n")

    response = requests.get(
        url,
        headers=auth_headers,
        params=params,
        timeout=30,
    )

    print(f"HTTP status: {response.status_code}")

    if not response.ok:
        print("\n[FAIL] Could not list model services")
        print("-" * 100)
        print(response.text)
        print("-" * 100)
        response.raise_for_status()

    data = response.json()

    services = data.get("model_services", [])

    print()
    print("=" * 100)
    print(f"AVAILABLE MODEL SERVICES: {len(services)}")
    print("=" * 100)

    if not services:
        print("\nNo accessible model services found in system.ai.")
        print(
            "\nPossible reasons:"
            "\n- Unity AI Gateway model services are not enabled for this workspace"
            "\n- workspace region does not support them"
            "\n- current user lacks required Unity Catalog permissions"
            "\n- required preview/entitlement is not enabled"
        )
        return

    for index, service in enumerate(services, start=1):
        resource_name = service.get("name", "UNKNOWN")
        api_types = service.get("supported_api_types", [])

        # API returns:
        # model-services/system.ai.some-model
        #
        # We need:
        # system.ai.some-model

        if resource_name.startswith("model-services/"):
            model_name = resource_name[len("model-services/"):]
        else:
            model_name = resource_name

        print(f"\n{index}. {model_name}")
        print(f"   API types: {api_types}")

    print()
    print("=" * 100)
    print("[PASS] MODEL SERVICE DISCOVERY FINISHED")
    print("=" * 100)


if __name__ == "__main__":
    main()
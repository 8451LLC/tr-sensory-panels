import argparse
import os
import sys

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.sensory.utils.databricks import get_workspace_client, list_schemas


def main():
    """
    Runs the list_schemas utility function and prints the results.
    """
    parser = argparse.ArgumentParser(
        description="List schemas in a Databricks catalog."
    )
    parser.add_argument("catalog_name", type=str, help="The name of the catalog.")
    args = parser.parse_args()

    client = get_workspace_client()
    schemas = list_schemas(client, args.catalog_name)

    if schemas:
        print(f"Available schemas in catalog '{args.catalog_name}':")
        for schema in schemas:
            print(f"- {schema}")
    else:
        print(f"No schemas found in catalog '{args.catalog_name}'.")


if __name__ == "__main__":
    main()

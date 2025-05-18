import argparse
import os
import sys

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.sensory.utils.databricks import get_workspace_client, list_tables


def main():
    """
    Runs the list_tables utility function and prints the results.
    """
    parser = argparse.ArgumentParser(description="List tables in a Databricks schema.")
    parser.add_argument("catalog_name", type=str, help="The name of the catalog.")
    parser.add_argument("schema_name", type=str, help="The name of the schema.")
    args = parser.parse_args()

    client = get_workspace_client()
    tables = list_tables(client, args.catalog_name, args.schema_name)

    if tables:
        print(f"Available tables in schema '{args.catalog_name}.{args.schema_name}':")
        for table in tables:
            print(f"- {table}")
    else:
        print(f"No tables found in schema '{args.catalog_name}.{args.schema_name}'.")


if __name__ == "__main__":
    main()

import argparse
import os
import sys

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.sensory.utils.databricks import get_workspace_client, list_catalogs


def main():
    """
    Runs the list_catalogs utility function and prints the results.
    """
    client = get_workspace_client()
    catalogs = list_catalogs(client)
    if catalogs:
        print("Available catalogs:")
        for catalog in catalogs:
            print(f"- {catalog}")
    else:
        print("No catalogs found.")


if __name__ == "__main__":
    main()

import pytest
from dotenv import load_dotenv
import os

from sensory.utils.databricks import get_workspace_client, execute_sql_query


@pytest.fixture(scope="session", autouse=True)
def load_env():
    """Load environment variables from .env file before any tests run."""
    load_dotenv()


def test_get_workspace_client_connects():
    """
    Tests that get_workspace_client can instantiate and connect.
    It performs a simple operation (listing current user's settings)
    to verify the connection.
    """
    client = None
    try:
        client = get_workspace_client()
        # Perform a simple operation to check the connection
        user_settings = client.current_user.me()
        assert user_settings is not None, "Should retrieve current user settings."
        print(
            "Successfully connected to Databricks. "
            f"Current user: {user_settings.user_name}"
        )
    except Exception as e:
        pytest.fail(
            "Failed to connect to Databricks Workspace or perform a basic "
            f"operation: {e}"
        )
    # No explicit close needed for WorkspaceClient as per typical SDK usage
    # unless specific resource cleanup is required by your operations.


def test_execute_sql_query_success():
    """
    Tests that execute_sql_query can successfully execute a query.
    It attempts to select the first row from a known table.
    """
    server_hostname = os.getenv("DATABRICKS_HOST")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    access_token = os.getenv("DATABRICKS_TOKEN")
    query = (
        "SELECT * FROM "
        "manufacturing_dev.work_agent_barney.master_sensory_responses_bronze "
        "LIMIT 1"
    )

    assert server_hostname, "DATABRICKS_HOST environment variable not set."
    assert http_path, "DATABRICKS_HTTP_PATH environment variable not set."
    assert access_token, "DATABRICKS_TOKEN environment variable not set."

    try:
        result = execute_sql_query(server_hostname, http_path, access_token, query)
        assert isinstance(result, list), "Query result should be a list."
        # We can't be sure if the table has data, but if it does, it should
        # have columns.
        # If the query runs successfully and the table is empty, result will
        # be an empty list.
        # If the table has data, result will be a list of tuples.
        if result:
            assert isinstance(result[0], tuple), "Query result rows should be tuples."
        print(f"Successfully executed SQL query. " f"Result has {len(result)} row(s).")
    except Exception as e:
        pytest.fail(f"execute_sql_query failed: {e}")

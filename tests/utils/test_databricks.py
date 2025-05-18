import pytest
from dotenv import load_dotenv

from sensory.utils.databricks import get_workspace_client


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
        # As of my last update, w.current_user.me() is a common way to get current user info.
        user_settings = client.current_user.me()
        assert user_settings is not None, "Should retrieve current user settings."
        print(
            f"Successfully connected to Databricks. Current user: {user_settings.user_name}"
        )
    except Exception as e:
        pytest.fail(
            f"Failed to connect to Databricks Workspace or perform a basic operation: {e}"
        )
    # No explicit close needed for WorkspaceClient as per typical SDK usage
    # unless specific resource cleanup is required by your operations.

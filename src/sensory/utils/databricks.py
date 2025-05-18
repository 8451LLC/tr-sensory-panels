from databricks.sdk import WorkspaceClient


def get_workspace_client() -> WorkspaceClient:
    """
    Instantiates a Databricks WorkspaceClient.

    Assumes that Databricks authentication environment variables are set
    (e.g., DATABRICKS_HOST and DATABRICKS_TOKEN).

    Returns:
        WorkspaceClient: An initialized Databricks WorkspaceClient.
    """
    return WorkspaceClient()

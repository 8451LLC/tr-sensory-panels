from databricks.sdk import WorkspaceClient
from databricks import sql


def get_workspace_client() -> WorkspaceClient:
    """
    Instantiates a Databricks WorkspaceClient.

    Assumes that Databricks authentication environment variables are set
    (e.g., DATABRICKS_HOST and DATABRICKS_TOKEN).

    Returns:
        WorkspaceClient: An initialized Databricks WorkspaceClient.
    """
    return WorkspaceClient()


def execute_sql_query(
    server_hostname: str, http_path: str, access_token: str, query: str
) -> list:
    """
    Connects to a Databricks SQL warehouse and executes a SQL query.

    Args:
        server_hostname: The server hostname of the Databricks SQL warehouse.
        http_path: The HTTP path of the Databricks SQL warehouse.
        access_token: The access token for authentication.
        query: The SQL query to execute.

    Returns:
        A list of tuples representing the query results.
    """
    with sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
    return result

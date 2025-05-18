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


def list_catalogs(client: WorkspaceClient) -> list[str]:
    """
    Lists all catalog names in the Unity Catalog.

    Args:
        client: An initialized Databricks WorkspaceClient.

    Returns:
        A list of catalog names.
    """
    return [c.name for c in client.catalogs.list() if c.name]


def list_schemas(client: WorkspaceClient, catalog_name: str) -> list[str]:
    """
    Lists all schema names within a specified catalog.

    Args:
        client: An initialized Databricks WorkspaceClient.
        catalog_name: The name of the catalog.

    Returns:
        A list of schema names.
    """
    return [
        s.name
        for s in client.schemas.list(catalog_name=catalog_name)
        if s.name
    ]


def list_tables(
    client: WorkspaceClient, catalog_name: str, schema_name: str
) -> list[str]:
    """
    Lists all table names within a specified catalog and schema.

    Args:
        client: An initialized Databricks WorkspaceClient.
        catalog_name: The name of the catalog.
        schema_name: The name of the schema.

    Returns:
        A list of table names.
    """
    return [
        t.name
        for t in client.tables.list(
            catalog_name=catalog_name, schema_name=schema_name
        )
        if t.name
    ]

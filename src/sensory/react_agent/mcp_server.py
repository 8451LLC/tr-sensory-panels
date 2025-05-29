"""
FastMCP server exposing SQLDatabaseToolkit functionalities.
"""

import uvicorn
from fastmcp.server import FastMCP
from pydantic import BaseModel, Field

# Import shared services
from sensory.react_agent.shared_services import get_sql_toolkit


# --- Pydantic Models for Input Validation ---
class QueryInput(BaseModel):
    query: str = Field(..., description="The SQL query to execute.")


class SchemaInput(BaseModel):
    tables: str = Field(
        ..., description="Comma-separated list of table names to get schema for."
    )


# --- Initialize FastMCP App ---
app = FastMCP()

# --- Initialize SQLDatabaseToolkit ---
# No longer needed here, use shared_services
# configuration = Configuration.from_context()
# engine = get_sqlalchemy_engine()
# db = SQLDatabase(
#     engine=engine,
#     lazy_table_reflection=True,
#     include_tables=configuration.include_tables,
# )
# # TODO: The llm instance needs to be passed here. This will be addressed by using shared_services.
# llm = load_chat_model(configuration.model) # Placeholder, will be replaced
# toolkit = SQLDatabaseToolkit(db=db, llm=llm)

sql_toolkit = get_sql_toolkit()
tools = sql_toolkit.get_tools()

# Find the specific tools from the toolkit
query_tool = next(tool for tool in tools if tool.name == "sql_db_query")
schema_tool = next(tool for tool in tools if tool.name == "sql_db_schema")
list_tables_tool = next(tool for tool in tools if tool.name == "sql_db_list_tables")
query_checker_tool = next(tool for tool in tools if tool.name == "sql_db_query_checker")


# --- MCP Tools ---
@app.tool(
    name="sql_query",
    description="Executes a SQL query against the database and returns the result. Use `sql_list_tables` to see available tables and `sql_get_schema` for table structures.",
)
async def sql_query_action(input: QueryInput) -> str:
    """Action to execute a SQL query."""
    return query_tool.run(input.query)


@app.tool(
    name="sql_get_schema",
    description="Returns the schema for a comma-separated list of tables. Ensure tables exist using `sql_list_tables` first.",
)
async def sql_get_schema_action(input: SchemaInput) -> str:
    """Action to get the schema of specified tables."""
    return schema_tool.run(input.tables)


@app.tool(
    name="sql_list_tables",
    description="Returns a comma-separated list of available tables in the database.",
)
async def sql_list_tables_action() -> str:
    """Action to list all tables in the database."""
    return list_tables_tool.run("")  # Input is an empty string for this tool


@app.tool(
    name="sql_query_checker",
    description="Checks the syntax of a SQL query. Use this before executing a query with `sql_query`.",
)
async def sql_query_checker_action(input: QueryInput) -> str:
    """Action to check the syntax of a SQL query."""
    return query_checker_tool.run(input.query)


# --- Uvicorn Runner for Local Testing ---
if __name__ == "__main__":
    # Get the ASGI app from FastMCP instance
    asgi_app = app.http_app()
    uvicorn.run(asgi_app, host="0.0.0.0", port=8000)

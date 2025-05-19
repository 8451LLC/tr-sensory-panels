# Plan: Integrating Langchain SQLDatabase Toolkit with Databricks

**Overall Goal:** Enable a Langchain agent to interact with a Databricks SQL warehouse using the `SQLDatabaseToolkit`, which requires a SQLAlchemy engine.

## Project Structure Review & Affected Files:

*   **`src/sensory/utils/databricks.py`**: This is the most critical file. A new function will be added here to create and return a SQLAlchemy engine configured for the Databricks SQL warehouse.
*   **`src/sensory/react_agent/tools.py`**: This file likely defines the tools the Langchain agent uses. It will be modified to:
    *   Import the new SQLAlchemy engine creation function.
    *   Initialize the Langchain `SQLDatabase` object.
    *   Initialize the `SQLDatabaseToolkit`.
    *   Provide tools from this toolkit to the agent.
*   **`src/sensory/react_agent/graph.py`** (or similar agent assembly file): If tools are assembled here, this file will be modified.
*   **`src/sensory/react_agent/configuration.py`**: May need updates for new configuration settings related to the database connection for Langchain.
*   **`pyproject.toml`**: Will be updated to include new dependencies: `langchain-community`, `SQLAlchemy`, and `sqlalchemy-databricks`.
*   **`tests/`**:
    *   **`tests/integration/react_agent/test_graph.py`**: Existing tests may need updates; new integration tests for the SQL toolkit will be added.
    *   **`tests/unit/utils/`** (new file or existing `test_databricks.py`): Unit tests for the new SQLAlchemy engine creation function.

## Detailed Plan:

### Phase 1: Setup and Dependencies

1.  **Update `pyproject.toml`**:
    *   Add dependencies:
        *   `langchain-community`
        *   `SQLAlchemy`
        *   `sqlalchemy-databricks` (Databricks dialect)
2.  **Install Dependencies**:
    *   Run `uv pip install .` (or equivalent for the project setup) to install/update dependencies.

### Phase 2: SQLAlchemy Engine Creation

1.  **Modify `src/sensory/utils/databricks.py`**:
    *   Import `create_engine` from `sqlalchemy`.
    *   Define `get_sqlalchemy_engine()`:
        *   Retrieve `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN` from environment variables.
        *   Construct SQLAlchemy connection URI: `databricks://token:<your_databricks_token>@<your_server_hostname>?http_path=<your_http_path>`
        *   Create and return SQLAlchemy engine: `create_engine(connection_uri)`.

### Phase 3: Langchain Integration

1.  **Modify `src/sensory/react_agent/tools.py`** (or agent assembly file):
    *   Import `SQLDatabase` from `langchain_community.utilities`.
    *   Import `SQLDatabaseToolkit` from `langchain_community.agent_toolkits`.
    *   Import `get_sqlalchemy_engine` from `src.sensory.utils.databricks`.
    *   In tool definition/provisioning:
        *   Get engine: `engine = get_sqlalchemy_engine()`.
        *   Create `SQLDatabase`: `db = SQLDatabase(engine=engine)`.
        *   Create `SQLDatabaseToolkit`: `toolkit = SQLDatabaseToolkit(db=db, llm=your_llm_instance)`.
        *   Get tools: `sql_tools = toolkit.get_tools()`.
        *   Add/replace existing database tools with `sql_tools`.

### Phase 4: Configuration and Agent Update (If Necessary)

1.  **Review/Modify `src/sensory/react_agent/configuration.py`**:
    *   Check for any specific configurations needed for the new SQL tools.
2.  **Review/Modify Agent Assembly (`src/sensory/react_agent/graph.py` or similar)**:
    *   Ensure the agent is initialized with the new SQLDatabaseToolkit tools.

### Phase 5: Testing

1.  **Add Unit Tests**:
    *   In `tests/unit/utils/` (e.g., `test_databricks.py`), add tests for `get_sqlalchemy_engine`. Mock environment variables and verify correct connection string usage.
2.  **Update/Add Integration Tests**:
    *   In `tests/integration/react_agent/test_graph.py`, adapt/add tests to ensure the agent can use SQL tools to query Databricks. (Requires test DB/table setup in Databricks).

### Phase 6: Refinement (Optional)

1.  **Refactor/Deprecate `SQLWarehouse`**:
    *   Post-stabilization, evaluate if the custom `SQLWarehouse` class is still necessary or if all SQL interactions can use the Langchain toolkit.


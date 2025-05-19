# Todo List: Databricks SQL Integration

This list is derived from the `databricks-sql-prd.md` document.

## Phase 1: Setup and Dependencies
- [x] **Update `pyproject.toml`**:
    - [x] Add `langchain-community` to dependencies.
    - [x] Add `SQLAlchemy` to dependencies.
    - [x] Add `sqlalchemy-databricks` (Databricks dialect) to dependencies.
- [x] **Install Dependencies**:
    - [x] Run `uv pip install .` (or equivalent for the project setup) to install/update dependencies.

## Phase 2: SQLAlchemy Engine Creation
- [x] **Modify `src/sensory/utils/databricks.py`**:
    - [x] Import `create_engine` from `sqlalchemy`.
    - [x] Define a new function `get_sqlalchemy_engine()`.
        - [x] Inside `get_sqlalchemy_engine()`, retrieve `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, and `DATABRICKS_TOKEN` from environment variables.
        - [x] Construct the SQLAlchemy connection URI in the format: `databricks://token:<your_databricks_token>@<your_server_hostname>?http_path=<your_http_path>`.
        - [x] Create and return the SQLAlchemy engine using `create_engine(connection_uri)`.

## Phase 3: Langchain Integration
- [x] **Modify `src/sensory/react_agent/tools.py`** (or the relevant agent assembly file, e.g., `graph.py`):
    - [x] Import `SQLDatabase` from `langchain_community.utilities`.
    - [x] Import `SQLDatabaseToolkit` from `langchain_community.agent_toolkits`.
    - [x] Import the new `get_sqlalchemy_engine` function from `src.sensory.utils.databricks`.
    - [x] In the tool definition or provisioning section:
        - [x] Get the SQLAlchemy engine by calling `get_sqlalchemy_engine()`.
        - [x] Create an `SQLDatabase` instance: `db = SQLDatabase(engine=engine)`.
        - [x] Create an `SQLDatabaseToolkit` instance: `toolkit = SQLDatabaseToolkit(db=db, llm=your_llm_instance)`. (Ensure `your_llm_instance` is correctly referenced/passed).
        - [x] Get the tools from the toolkit: `sql_tools = toolkit.get_tools()`.
        - [x] Add the `sql_tools` to the agent's available tools, potentially replacing existing custom database tools.

## Phase 4: Configuration and Agent Update (If Necessary)
- [x] **Review/Modify `src/sensory/react_agent/configuration.py`**:
    - [x] Check if any new configuration settings are needed for the SQL tools (e.g., table inclusions, sample rows).
- [x] **Review/Modify Agent Assembly (`src/sensory/react_agent/graph.py` or similar)**:
    - [x] Ensure the agent is correctly initialized or updated to use the new `SQLDatabaseToolkit` tools.

## Phase 5: Testing
- [ ] **Add Unit Tests**:
    - [ ] In `tests/unit/utils/` (create or use existing `test_databricks.py`):
        - [ ] Add unit tests for the `get_sqlalchemy_engine` function.
        - [ ] Mock environment variables for `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN`.
        - [ ] Verify that the connection URI is constructed correctly.
        - [ ] Verify that `create_engine` is called with the correct URI.
- [ ] **Update/Add Integration Tests**:
    - [ ] In `tests/integration/react_agent/test_graph.py`:
        - [ ] Adapt existing tests or add new integration tests to verify the agent can use the SQL tools.
        - [ ] These tests should ideally query a test Databricks SQL warehouse (requires setup of a test database/table in Databricks).
        - [ ] Test various SQL operations (e.g., list tables, query schema, run queries).

## Phase 6: Refinement (Optional)
- [ ] **Refactor/Deprecate `SQLWarehouse` class**:
    - [ ] After the new toolkit is integrated and stable, evaluate if the custom `SQLWarehouse` class in `src/sensory/utils/databricks.py` is still needed.
    - [ ] If its functionality is fully covered by the Langchain toolkit, plan for its deprecation and removal.

\
# Checklist: Embeddings for Sensory Panel Data

## Phase 1: Core Embedding Generation

### Data Sources & Schema
- [ ] Identify `data` column in `master_sensory_panel_joined_silver`.
- [ ] Identify `data` column in `master_sensory_responses_collected_silver`.
- [ ] Decision: Store embeddings in a new column `data_embedding ARRAY<FLOAT>` in the respective tables.
- [ ] Action: Manually add `data_embedding ARRAY<FLOAT>` column to `master_sensory_panel_joined_silver` (if not already done).
- [ ] Action: Manually add `data_embedding ARRAY<FLOAT>` column to `master_sensory_responses_collected_silver` (if not already done).

### Embedding Process - Script (`generate_embeddings.py`)
- [ ] Use Python.
- [ ] Leverage `SQLWarehouse` for Databricks interaction (data retrieval and updates).
- [ ] Serialize `data` column content to JSON string for embedding.
- [ ] Handle null/missing `data` columns gracefully (serialized to empty string).
- [ ] Utilize OpenAI `text-embedding-3-small` model.
- [ ] Use official OpenAI Python SDK.
- [ ] Implement batching for sending data to OpenAI API.
- [ ] Handle potential errors during embedding (API errors, rate limits) with retries/logging.
- [ ] Ensure process is idempotent (skip if `data_embedding` is not NULL) or allows overwrite.
- [ ] Structure code logically in `src/sensory/embeddings/`.
- [ ] Review and enhance comments and documentation within `generate_embeddings.py`.
- [ ] Configure and test appropriate batch sizes (`openai_api_batch_size`, `db_fetch_batch_size`, `db_update_batch_size`) for production volumes.
- [ ] Verify error handling and logging are robust for all edge cases.

### Configuration & Assumptions
- [ ] Databricks credentials (`DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN`) assumed to be in environment.
- [ ] OpenAI API key (`OPENAI_API_KEY`, `OPENAI_API_BASE`) assumed to be in environment.
- [ ] `SQLWarehouse` utility is functional for SELECT and UPDATE DML.
- [ ] Verify serialized `data` column content does not exceed `text-embedding-3-small` token limits for typical records.

### Deliverables (Core Embedding)
- [ ] Python script `generate_embeddings.py`.
- [ ] Project Requirement Document `docs/embeddings-prd.md`.
- [ ] Jupyter notebook `scratch/embeddings.ipynb` for experimentation.
- [ ] This checklist document `docs/embeddings-todo.md`.

## Phase 2: Databricks Vector Search Setup (Post-Embedding Population)

### Prerequisites
- [ ] Confirm Unity Catalog is enabled in the workspace.
- [ ] Confirm Serverless Compute is enabled.
- [ ] Enable Change Data Feed (CDF) on `master_sensory_panel_joined_silver`.
- [ ] Enable Change Data Feed (CDF) on `master_sensory_responses_collected_silver`.
- [ ] Ensure appropriate permissions for creating Vector Search endpoints and indexes.

### Vector Search Endpoint
- [ ] Create a Vector Search Endpoint (e.g., `sensory_data_vector_search_endpoint`) via UI or SDK.

### Delta Sync Indexes
- [ ] Create Delta Sync Index for `master_sensory_panel_joined_silver`.
    - Endpoint: (e.g., `sensory_data_vector_search_endpoint`)
    - Source Table: `manufacturing_dev.work_agent_barney.master_sensory_panel_joined_silver`
    - Primary Key(s): `item_spec_number`
    - Embedding Column: `data_embedding`
    - Pipeline Type: `CONTINUOUS` (recommended) or `TRIGGERED`
- [ ] Create Delta Sync Index for `master_sensory_responses_collected_silver`.
    - Endpoint: (e.g., `sensory_data_vector_search_endpoint`)
    - Source Table: `manufacturing_dev.work_agent_barney.master_sensory_responses_collected_silver`
    - Primary Key(s): `test_id`, `unique_panelist_id` (or derived single PK)
    - Embedding Column: `data_embedding`
    - Pipeline Type: `CONTINUOUS` (recommended) or `TRIGGERED`

### Querying & Validation
- [ ] Develop example queries to test semantic search on the indexes.
- [ ] Validate search results.
